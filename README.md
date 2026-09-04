# HighPerfImageEngine

🇧🇷 Português | 🇺🇸 [English](README.en.md)

Um consumidor RabbitMQ em .NET que decodifica imagens, aplica um filtro de brilho via SIMD (AVX2) e reencoda para WebP — rodando dentro de um orçamento de CPU e memória explicitamente pequeno (o padrão do projeto simula **2 vCPUs / 512MB de RAM**, próximo de uma instância AWS t3.nano), com alocação de memória gerenciada próxima de zero.

## O que ele faz

- Valida a imagem recebida (checagem de assinatura/magic numbers)
- Aplica um filtro de brilho usando instruções SIMD (AVX2, 32 bytes de pixel por vez)
- Reencoda para WebP
- Processa tudo isso com **~0,56 KB de alocação gerenciada por mensagem** e o coletor de lixo praticamente parado

## Como rodar

Pré-requisitos: Docker e Docker Compose.

```bash
git clone https://github.com/Victor2043/dotnet-high-perf-image-engine.git
cd dotnet-high-perf-image-engine
docker-compose up --build
```

Acompanhe o processamento em tempo real pelo painel do RabbitMQ em `http://localhost:15672` (usuário/senha: `guest`/`guest`). Ao final, o `dotnet-engine` imprime um relatório com throughput, alocação de memória e coletas de GC.

Para limpar tudo:

```bash
docker-compose down -v
```

> **Atenção:** o `-v` remove volumes Docker. Se você usa Docker para outros projetos na mesma máquina, prefira remover apenas os containers/volumes específicos deste repositório em vez de rodar comandos de limpeza globais.

## Quer entender como e por quê?

Este README é só a superfície. Se você quer entender a arquitetura interna, as decisões de design (incluindo por que este projeto **não** usa `BackgroundService`/`IHostedService`), e a jornada completa de otimização — os bugs encontrados, os becos sem saída, e como cada gargalo foi diagnosticado com dados reais em vez de suposição — tem um documento bem mais longo e sem papas na língua esperando por você:

📖 **[Leia o mergulho técnico completo em `docs/DEEP_DIVE.md`](docs/DEEP_DIVE.md)**

## Licença

Este projeto está licenciado sob a MIT License.