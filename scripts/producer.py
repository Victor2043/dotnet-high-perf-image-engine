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


def create_connection(rabbit_host):
    """Create a RabbitMQ connection with retry logic."""
    for attempt in range(1, 16):
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
                f"{attempt}/15 failed: {exc}"
            )

            time.sleep(1)

    return None


def worker_publisher(
    worker_id,
    rabbit_host,
    duration_seconds,
    image_cache,
    counter,
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

    properties = pika.BasicProperties(
        delivery_mode=2,
        content_type="application/json",
    )

    start_time = time.time()
    local_sent = 0

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

            channel.basic_publish(
                exchange=MAIN_EXCHANGE,
                routing_key=MAIN_ROUTING_KEY,
                body=json.dumps(payload),
                properties=properties,
            )

            local_sent += 1

            # Update the shared counter every 100 messages
            # to reduce synchronization overhead.
            if local_sent >= 100:
                with counter.get_lock():
                    counter.value += local_sent

                local_sent = 0

        # Flush remaining messages.
        if local_sent > 0:
            with counter.get_lock():
                counter.value += local_sent

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

        # Enable publisher confirms only for the final marker.
        # This avoids affecting the image publishing benchmark.
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
                f"Sent: {current_count} msgs | "
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

    print("\n")

    print("[Python Producer] Publishing phase completed!")
    print(
        f"-> Total image messages published: {total_messages}"
    )
    print(
        f"-> Publishing time: {total_time:.2f} seconds"
    )

    if total_time > 0:
        print(
            f"-> Average throughput: "
            f"{total_messages / total_time:.2f} msgs/sec"
        )

    # IMPORTANT:
    # The completion marker is published only after all
    # worker processes have finished.
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