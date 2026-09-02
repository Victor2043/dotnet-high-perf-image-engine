import os
import time
import random
import json
import base64
import multiprocessing as mp
import pika

def load_images_to_memory(input_dir):
    """Carrega e codifica em Base64 todas as imagens em memória uma única vez."""
    valid_extensions = ('.jpg', '.jpeg', '.png', '.webp')
    image_files = [
        f for f in os.listdir(input_dir) 
        if f.lower().endswith(valid_extensions)
    ]

    if not image_files:
        return []

    print(f"[Python Producer] Pré-carregando {len(image_files)} imagens para a memória RAM...")
    cache = []
    for file_name in image_files:
        file_path = os.path.join(input_dir, file_name)
        with open(file_path, "rb") as img_file:
            encoded_bytes = base64.b64encode(img_file.read()).decode('utf-8')
            cache.append({
                "file_name": file_name,
                "content_base64": encoded_bytes
            })
    print(f"[Python Producer] {len(cache)} imagens carregadas e codificadas em memória!")
    return cache

def worker_publisher(worker_id, rabbit_host, duration_seconds, image_cache, counter):
    """Worker que roda em um processo separado mantendo uma conexão TCP ativa com RabbitMQ."""
    connection = None
    for _ in range(15):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbit_host,
                    # Habilita TCP Keepalive e otimizações de socket
                    tcp_options=pika.ConnectionParameters.DEFAULT_TCP_OPTIONS
                )
            )
            break
        except Exception:
            time.sleep(1)

    if not connection:
        print(f"[Worker {worker_id}] Falha ao conectar ao RabbitMQ.")
        return

    channel = connection.channel()
    main_exchange = "image.events"
    main_routing_key = "image.process"

    # Declara exchange caso ainda não exista
    channel.exchange_declare(exchange=main_exchange, exchange_type='direct', durable=True)

    properties = pika.BasicProperties(
        delivery_mode=2,
        content_type='application/json'
    )

    start_time = time.time()
    local_sent = 0

    while (time.time() - start_time) < duration_seconds:
        # Seleciona um item pré-carregado em memória (Zero I/O de disco)
        item = random.choice(image_cache)

        payload = {
            "file_name": item["file_name"],
            "content_base64": item["content_base64"],
            "brightness_offset": random.randint(30, 80)
        }

        channel.basic_publish(
            exchange=main_exchange,
            routing_key=main_routing_key,
            body=json.dumps(payload),
            properties=properties
        )

        local_sent += 1

        # Atualiza o contador compartilhado a cada 100 envios para minimizar overhead de Lock
        if local_sent % 100 == 0:
            with counter.get_lock():
                counter.value += 100
            local_sent = 0

    # Atualiza o restante das mensagens pendentes no contador
    if local_sent > 0:
        with counter.get_lock():
            counter.value += local_sent

    connection.close()

def main():
    rabbit_host = os.getenv("RABBITMQ_HOST", "localhost")
    duration_seconds = int(os.getenv("DURATION_SECONDS", "10"))
    input_dir = os.getenv("INPUT_DIR", "/app/input_files")

    # Garante que usamos no mínimo 2 processos e no máximo a quantidade de CPUs lógicas disponíveis
    num_workers = max(2, mp.cpu_count())

    image_cache = load_images_to_memory(input_dir)
    if not image_cache:
        print(f"[Python Producer] Nenhuma imagem encontrada em {input_dir}")
        return

    print(f"[Python Producer] Iniciando estresse com {num_workers} processos concorrentes durante {duration_seconds} segundos...")

    counter = mp.Value('i', 0)
    processes = []
    start_time = time.time()

    # Cria e inicia o pool de processos
    for i in range(num_workers):
        p = mp.Process(
            target=worker_publisher,
            args=(i, rabbit_host, duration_seconds, image_cache, counter)
        )
        processes.append(p)
        p.start()

    # Monitor de progresso no terminal em tempo real
    while any(p.is_alive() for p in processes):
        elapsed = time.time() - start_time
        if elapsed > 0:
            cps = counter.value / elapsed
            print(f"\r[Python Producer] Tempo: {elapsed:.1f}s / {duration_seconds}s | Total Enviado: {counter.value} msgs | Throughput: {cps:.1f} msgs/seg", end="", flush=True)
        time.sleep(0.5)

    for p in processes:
        p.join()

    total_time = time.time() - start_time
    print(f"\n\n[Python Producer] Concluído!")
    print(f"-> Total de imagens enviadas: {counter.value}")
    print(f"-> Tempo total de execução: {total_time:.2f} segundos")
    print(f"-> Média de vazão: {counter.value / total_time:.2f} msgs/seg")

if __name__ == "__main__":
    main()