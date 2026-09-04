# HighPerfImageEngine

🇺🇸 English | 🇧🇷 [Português](README.md)

A .NET RabbitMQ consumer that decodes images, applies a brightness filter via SIMD (AVX2), and re-encodes to WebP — running within an explicitly small CPU/memory budget (the project defaults to **2 vCPUs / 512MB RAM**, close to an AWS t3.nano instance), with managed memory allocation near zero.

## What it does

- Validates the incoming image (magic-number/signature check)
- Applies a brightness filter using SIMD instructions (AVX2, 32 pixel bytes at a time)
- Re-encodes to WebP
- Does all of this with **~0.56 KB of managed allocation per message**, with the garbage collector barely ever running

## How to run

Prerequisites: Docker and Docker Compose.

```bash
git clone https://github.com/Victor2043/dotnet-high-perf-image-engine.git
cd dotnet-high-perf-image-engine
docker-compose up --build
```

Watch the processing live via the RabbitMQ management UI at `http://localhost:15672` (user/password: `guest`/`guest`). When it's done, `dotnet-engine` prints a report with throughput, memory allocation, and GC collections.

To clean everything up:

```bash
docker-compose down -v
```

> **Warning:** `-v` removes Docker volumes. If you use Docker for other projects on the same machine, prefer removing only this repository's specific containers/volumes instead of running a global cleanup command.

## Want to understand how and why?

This README is just the surface. If you want to understand the internal architecture, the design decisions (including why this project **doesn't** use `BackgroundService`/`IHostedService`), and the full optimization journey — the bugs found, the dead ends, and how every bottleneck was diagnosed with real data instead of guesswork — there's a much longer, no-punches-pulled document waiting for you:

📖 **[Read the full technical deep dive in `docs/DEEP_DIVE.en.md`](docs/DEEP_DIVE.en.md)**

## License

This project is licensed under the MIT License.