import os
import time
import random
import json
import base64
import uuid
import multiprocessing as mp

import pika


MAIN_EXCHANGE = "image.events"
MAIN_ROUTING_KEY = "image.process"
BATCH_COMPLETED_MESSAGE_TYPE = "batch_completed"


def load_images_to_memory(input_dir):
    """Load and Base64-encode all images into memory once."""
    valid_extensions = (".jpg", ".jpeg", ".png", ".webp")

    image_files = [
        file_name
        for file_name in os.listdir(input_dir)
        if file_name.lower().endswith(valid_extensions)
    ]

    if not image_files:
        return []

    print(
        f"[Python Producer] Preloading "
        f"{len(image_files)} images into RAM..."
    )

    cache = []

    for file_name in image_files:
        file_path = os.path.join(input_dir, file_name)

        with open(file_path, "rb") as image_file:
            encoded_bytes = base64.b64encode(
                image_file.read()
            ).decode("utf-8")

            cache.append(
                {
                    "file_name": file_name,
                    "content_base64": encoded_bytes,
                }
            )

    print(
        f"[Python Producer] "
        f"{len(cache)} images loaded and Base64-encoded in memory!"
    )

    return cache


def create_connection(rabbit_host, max_attempts=40, delay_seconds=2):
    """
    Create a RabbitMQ connection with retry logic.

    A generous budget on purpose: RabbitMQ's own boot time varies a lot
    depending on the machine and whether it's initializing its Mnesia
    database for the first time (a fresh rabbitmq_data volume can take well
    over 20 seconds to become ready). The Docker healthcheck can also report
    "healthy" slightly before the AMQP listener itself is accepting
    connections, since it only pings the Erlang node. 40 attempts * 2s
    gives up to ~80 seconds of slack, which comfortably covers a slow first
    boot without hanging forever on a genuinely broken broker.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbit_host,
                    tcp_options=pika.ConnectionParameters.DEFAULT_TCP_OPTIONS,
                )
            )

            return connection

        except Exception as exc:
            print(
                f"[RabbitMQ] Connection attempt "
                f"{attempt}/{max_attempts} failed: {exc}"
            )

            time.sleep(delay_seconds)

    return None


def worker_publisher(
    worker_id,
    rabbit_host,
    duration_seconds,
    image_cache,
    counter,
    failed_counter,
):
    """Publish images from an independent worker process."""
    connection = create_connection(rabbit_host)

    if connection is None:
        print(
            f"[Worker {worker_id}] "
            f"Failed to connect to RabbitMQ."
        )
        return

    channel = connection.channel()

    channel.exchange_declare(
        exchange=MAIN_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    # Publisher confirms turn "I called basic_publish" into "the broker
    # actually accepted and routed this message". Without this, a
    # successful basic_publish() call means nothing on its own: a swallowed
    # channel error, a dropped connection, or the broker rejecting the
    # message under memory pressure all look identical to success from the
    # caller's point of view, and the local counter below would silently
    # overcount messages that never actually reached the queue.
    #
    # This does turn each publish into a confirmed round-trip, which lowers
    # THIS script's own reported publish throughput. That trade-off is
    # intentional and does not affect the .NET consumer's own throughput
    # measurement, which is timed independently on its side based on what
    # it actually received and processed.
    channel.confirm_delivery()

    properties = pika.BasicProperties(
        delivery_mode=2,
        content_type="application/json",
    )

    start_time = time.time()
    local_sent = 0
    local_failed = 0

    try:
        while (time.time() - start_time) < duration_seconds:
            # Select an image already loaded in memory.
            # No disk I/O occurs during the benchmark.
            item = random.choice(image_cache)

            payload = {
                "file_name": item["file_name"],
                "content_base64": item["content_base64"],
                "brightness_offset": random.randint(30, 80),
            }

            try:
                channel.basic_publish(
                    exchange=MAIN_EXCHANGE,
                    routing_key=MAIN_ROUTING_KEY,
                    body=json.dumps(payload),
                    properties=properties,
                    mandatory=True,
                )

                local_sent += 1

            except (pika.exceptions.UnroutableError, pika.exceptions.NackError):
                # The broker rejected or couldn't route this one (e.g. under
                # memory pressure, or briefly before the consumer has
                # declared the queue). Count it honestly instead of
                # pretending it made it through.
                local_failed += 1

            # Update the shared counters every 100 messages to reduce
            # synchronization overhead.
            if local_sent + local_failed >= 100:
                with counter.get_lock():
                    counter.value += local_sent

                with failed_counter.get_lock():
                    failed_counter.value += local_failed

                local_sent = 0
                local_failed = 0

        # Flush remaining messages.
        if local_sent > 0:
            with counter.get_lock():
                counter.value += local_sent

        if local_failed > 0:
            with failed_counter.get_lock():
                failed_counter.value += local_failed

    finally:
        connection.close()


def publish_batch_completed_message(
    rabbit_host,
    batch_id,
    expected_messages,
):
    """
    Publish the final batch marker.

    This message is published only after every worker process
    has finished publishing its image messages.
    """

    connection = create_connection(rabbit_host)

    if connection is None:
        raise RuntimeError(
            "Could not connect to RabbitMQ to publish "
            "the batch completion message."
        )

    try:
        channel = connection.channel()

        channel.exchange_declare(
            exchange=MAIN_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

        channel.confirm_delivery()

        properties = pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        )

        payload = {
            "message_type": BATCH_COMPLETED_MESSAGE_TYPE,
            "batch_id": batch_id,
            "expected_messages": expected_messages,
        }

        channel.basic_publish(
            exchange=MAIN_EXCHANGE,
            routing_key=MAIN_ROUTING_KEY,
            body=json.dumps(payload),
            properties=properties,
            mandatory=True,
        )

        print(
            f"[Python Producer] Batch completion marker confirmed by RabbitMQ. "
            f"Batch ID: {batch_id}"
        )

    finally:
        connection.close()


def wait_until_queue_is_ready(rabbit_host, timeout_seconds=60):
    """
    Blocks until a message can actually be routed to MAIN_ROUTING_KEY on
    MAIN_EXCHANGE.

    The .NET consumer is the one that declares the exchange/queue/binding on
    startup, and Docker Compose has no built-in way to wait for "the other
    service finished declaring its own internal topology" (only "the
    container started" or "a health check passed"). Without this, the
    producer can start blasting messages before the queue exists at all,
    and every one of those early messages is silently unroutable — which is
    exactly what was showing up as a big chunk of "failed" publishes.

    This sends one small, harmless probe message (repeatedly, until it's
    routed) using the same mandatory+confirm_delivery mechanism as the real
    traffic. The .NET side will receive exactly one of these as a stray,
    unparseable message and forward it to the DLQ — expected and harmless.
    """
    connection = create_connection(rabbit_host)

    if connection is None:
        raise RuntimeError("Could not connect to RabbitMQ to check readiness.")

    try:
        channel = connection.channel()

        channel.exchange_declare(
            exchange=MAIN_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

        channel.confirm_delivery()

        deadline = time.time() + timeout_seconds
        attempt = 0

        while time.time() < deadline:
            attempt += 1

            try:
                channel.basic_publish(
                    exchange=MAIN_EXCHANGE,
                    routing_key=MAIN_ROUTING_KEY,
                    body=json.dumps({"message_type": "readiness_probe"}),
                    properties=pika.BasicProperties(delivery_mode=1),
                    mandatory=True,
                )

                print(
                    f"[Python Producer] Consumer queue is ready "
                    f"(probe succeeded after {attempt} attempt(s))."
                )
                return

            except (pika.exceptions.UnroutableError, pika.exceptions.NackError):
                print(
                    f"[Python Producer] Waiting for the consumer to declare "
                    f"its queue... (attempt {attempt})"
                )
                time.sleep(1)

        raise RuntimeError(
            f"Consumer queue never became routable within {timeout_seconds}s. "
            "Is the dotnet-engine service up and connected to RabbitMQ?"
        )
    finally:
        connection.close()


def main():
    rabbit_host = os.getenv("RABBITMQ_HOST", "localhost")
    duration_seconds = int(
        os.getenv("DURATION_SECONDS", "10")
    )
    input_dir = os.getenv(
        "INPUT_DIR",
        "/app/input_files",
    )

    # Use at least 2 processes and up to the number
    # of logical CPUs available.
    num_workers = max(2, mp.cpu_count())

    image_cache = load_images_to_memory(input_dir)

    if not image_cache:
        print(
            f"[Python Producer] "
            f"No images found in {input_dir}"
        )
        return

    print(
        "[Python Producer] Waiting for the consumer to be ready "
        "to receive messages..."
    )
    wait_until_queue_is_ready(rabbit_host)

    batch_id = str(uuid.uuid4())

    print(
        f"[Python Producer] Starting stress test with "
        f"{num_workers} concurrent processes for "
        f"{duration_seconds} seconds..."
    )

    print(
        f"[Python Producer] Batch ID: {batch_id}"
    )

    counter = mp.Value("q", 0)
    failed_counter = mp.Value("q", 0)

    processes = []

    start_time = time.time()

    # Start worker processes.
    for worker_id in range(num_workers):
        process = mp.Process(
            target=worker_publisher,
            args=(
                worker_id,
                rabbit_host,
                duration_seconds,
                image_cache,
                counter,
                failed_counter,
            ),
        )

        processes.append(process)
        process.start()

    # Live progress monitoring.
    while any(process.is_alive() for process in processes):
        elapsed = time.time() - start_time

        if elapsed > 0:
            current_count = counter.value
            throughput = current_count / elapsed

            print(
                f"\r[Python Producer] "
                f"Elapsed: {elapsed:.1f}s / {duration_seconds}s | "
                f"Confirmed: {current_count} msgs | "
                f"Failed: {failed_counter.value} | "
                f"Throughput: {throughput:.1f} msgs/sec",
                end="",
                flush=True,
            )

        time.sleep(0.5)

    # Wait for every worker to finish.
    for process in processes:
        process.join()

    total_time = time.time() - start_time
    total_messages = counter.value
    total_failed = failed_counter.value

    print("\n")

    print("[Python Producer] Publishing phase completed!")
    print(
        f"-> Total image messages confirmed by broker: {total_messages}"
    )

    if total_failed > 0:
        print(
            f"-> Messages rejected/unroutable (NOT counted as sent): {total_failed}"
        )

    print(
        f"-> Publishing time: {total_time:.2f} seconds"
    )

    if total_time > 0:
        print(
            f"-> Average confirmed throughput: "
            f"{total_messages / total_time:.2f} msgs/sec"
        )

    # IMPORTANT:
    # The completion marker is published only after all
    # worker processes have finished, and expected_messages now reflects
    # only messages the broker actually confirmed — not just how many
    # times basic_publish() was called.
    print(
        "[Python Producer] Publishing final batch completion marker..."
    )

    try:
        publish_batch_completed_message(
            rabbit_host=rabbit_host,
            batch_id=batch_id,
            expected_messages=total_messages,
        )

        print(
            "[Python Producer] Batch is now fully published."
        )

    except Exception as exc:
        print(
            f"[Python Producer] "
            f"Failed to publish batch completion marker: {exc}"
        )
        raise


if __name__ == "__main__":
    main()