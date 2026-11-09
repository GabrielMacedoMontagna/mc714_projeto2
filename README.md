# MC714 — Trabalho Prático 2: Coordenação e Consistência em Sistemas Distribuídos

**Disciplina:** MC714 — Sistemas Distribuídos (2º Semestre de 2026)  
**Instituição:** Instituto de Computação — Universidade Estadual de Campinas (UNICAMP)  
**Professor:** Prof. Dr. L. F. Bittencourt  

## Integrantes da Dupla

- **Gabriel de Macedo Cavassani Montagna** — RA: 242352
- **Nicholas Pucharelli Fontanini** — RA: 253769

---

## 1. Visão Geral e Arquitetura do Sistema

Este projeto implementa um **Serviço de Armazenamento Chave-Valor Replicado com Coordenação de Seção Crítica e Detecção de Falhas** em Python 3.12+, estruturado segundo a **Arquitetura Hexagonal (*Ports and Adapters*)** e utilizando comunicação assíncrona via **gRPC (`grpc.aio`) + Protocol Buffers** sobre TCP/IP.

O sistema integra de forma coesa **três algoritmos clássicos de Sistemas Distribuídos** (dois obrigatórios + um algoritmo bônus):

1. **Grupo 1 — Exclusão Mútua Distribuída de Ricart-Agrawala com Relógios Lógicos de Lamport** (`src/mc714_p2/domain/clock.py` e `src/mc714_p2/domain/mutex.py`):
   - Ordenação causal total de requisições usando pares `(lamport_time, node_id)` com desempate determinístico lexicográfico.
   - Complexidade de mensagens $2(N - 1)$ por entrada na seção crítica (`RequestAccess` + respostas imediatas ou diferidas via `DeliverDeferredReply`).
   - Registro distribuído de auditoria (`CriticalSectionRecord`) para verificação formal da propriedade de *Safety* (zero sobreposições na seção crítica).
2. **Grupo 2 — Replicação Baseada em Quorum de Gifford com *Read-Repair*** (`src/mc714_p2/domain/quorum.py`):
   - Configuração parametrizável $(N, N_R, N_W)$ com validação estrita das invariantes de consistência forte: $N_R + N_W > N$ e $N_W > \lfloor N/2 \rfloor$.
   - Escrita coordenada em duas fases: consulta de versões ao quorum $N_W$, cálculo de $\text{next\_version} = \max(v) + 1$ e propagação atômica da nova tupla `(value, version, writer_id, lamport_ts)`.
   - Leitura em quorum $N_R$ com seleção da maior versão e disparo automático de **Read-Repair** para atualizar réplicas defasadas (*stale*) detectadas no quorum.
3. **Algoritmo 3 (Bônus) — Detector de Falhas por *Heartbeat* Integrado à Exclusão Mútua** (`src/mc714_p2/domain/heartbeat.py` e `src/mc714_p2/node/coordinator.py`):
   - Monitoramento periódico assíncrono (`SendHeartbeat`) com parâmetros configuráveis de intervalo ($\Delta t_{\text{hb}}$) e *timeout* de suspeita ($T_{\text{suspect}}$).
   - Integração direta com o algoritmo de Ricart-Agrawala: quando um nó sofre falha por parada (*crash-stop*) e transiciona para `SUSPECTED`, o coordenador remove o nó suspeito do conjunto `pending_replies`, restaurando a propriedade de **Liveness** da seção crítica sem violar *Safety*.
4. **Injetor de Falhas Determinístico Reprodutível** (`src/mc714_p2/domain/fault_injector.py`):
   - Suporte via RPC (`ConfigureFault`) para simulação controlada de *crash-stop*, partições assimétricas/simétricas, descarte probabilístico de pacotes e atraso/jitter de rede com semente pseudoaleatória explícita (`seed`).

---

## 2. Estrutura do Repositório

```text
.
├── compose.yaml                         # Orquestração Docker Compose v2 (cluster de 5 nós)
├── Dockerfile                           # Imagem OCI multi-stage com Python 3.12 e uv
├── pyproject.toml                       # Dependências, Ruff, Pyright e Pytest
├── relatorio/
│   ├── main.tex                         # Código-fonte LaTeX do relatório (formato IEEEtran)
│   └── main.pdf                         # Relatório técnico compilado (máx. 6 páginas + refs)
├── results/
│   ├── raw/experiments_raw.json         # Dados brutos e metadados de hardware/SO/sementes
│   ├── tables/*.csv                     # Tabelas estatísticas (média, DP, p50, p95, p99)
│   └── figures/*.pdf                    # Gráficos vetoriais gerados automaticamente
├── scripts/
│   ├── demo_cluster.py                  # Roteiro automatizado de demonstração (normal + falhas)
│   └── run_experiments.py               # Executor oficial da suíte experimental
├── src/mc714_p2/
│   ├── cli.py                           # Ponto de entrada de linha de comando do nó
│   ├── domain/                          # Núcleo puro dos algoritmos (sem dependências de rede)
│   │   ├── clock.py                     # Relógio Lógico de Lamport e LogicalTimestamp
│   │   ├── fault_injector.py            # Injetor de falhas determinístico (crash, drop, delay)
│   │   ├── heartbeat.py                 # Detector de falhas por Heartbeat (ALIVE / SUSPECTED)
│   │   ├── metrics.py                   # Cálculo de percentis (p50, p95, p99) e Safety/Consistency
│   │   ├── mutex.py                     # Exclusão Mútua de Ricart-Agrawala
│   │   └── quorum.py                    # Quorum de Gifford (NR, NW) e Read-Repair
│   ├── experiments/                     # Suíte de benchmark e geração de gráficos
│   │   ├── plotting.py                  # Geração de gráficos Matplotlib (PDF/PNG) e CSVs
│   │   └── runner.py                    # Orquestrador dos 3 experimentos com warm-up e réplicas
│   ├── node/                            # Adaptadores de coordenação assíncrona do nó
│   │   ├── config.py                    # Configuração via variáveis de ambiente e CLI
│   │   ├── coordinator.py               # Orquestrador de fluxos distribuídos e Heartbeat loop
│   │   └── grpc_servicer.py             # Implementação dos handlers gRPC assíncronos
│   └── rpc/                             # Camada de comunicação gRPC + Protobuf
│       ├── client.py                    # Cliente gRPC assíncrono com deadlines explícitos obrigatórios
│       ├── cluster.proto                # Definição formal das mensagens e serviços Protobuf
│       └── proto_gen/                   # Stubs gerados pelo grpc_tools.protoc
└── tests/
    ├── unit/                            # Testes unitários determinísticos da camada de domínio e RPC
    └── integration/                     # Testes de integração multi-nó reais via gRPC sobre TCP/IP
```

---

## 3. Pré-requisitos e Instalação Local

### Pré-requisitos
- **Python 3.12+** e gerenciador de pacotes [**uv**](https://docs.astral.sh/uv/)
- **Docker Engine** e **Docker Compose v2** (`docker compose version`)

### Instalação do Ambiente de Desenvolvimento
```bash
# Instala todas as dependências de produção e desenvolvimento em .venv/
uv sync --all-extras
```

---

## 4. Variáveis de Ambiente e Parâmetros de Configuração

Cada nó (`mc714-node`) pode ser configurado integralmente via variáveis de ambiente ou argumentos de CLI, permitindo execução transparente tanto no Docker Compose local quanto em máquinas virtuais da nuvem privada do IC:

| Variável de Ambiente | Flag CLI | Padrão | Descrição |
| :--- | :--- | :--- | :--- |
| `NODE_ID` | `--node-id` | *(obrigatório)* | Identificador único do nó (ex.: `node-1`) |
| `HOST` | `--host` | `0.0.0.0` | Endereço IP de escuta do servidor gRPC |
| `PORT` | `--port` | `50051` | Porta TCP do servidor gRPC |
| `ADVERTISE_HOST` | `--advertise-host` | `127.0.0.1` | Host anunciado para os pares do cluster |
| `PEERS` | `--peers` | `""` | Lista de pares `node_id=host:port` separados por vírgula |
| `QUORUM_R` | `--quorum-r` | `3` | Tamanho do quorum de leitura ($N_R$) |
| `QUORUM_W` | `--quorum-w` | `3` | Tamanho do quorum de escrita ($N_W$) |
| `HB_INTERVAL_MS` | `--hb-interval-ms` | `150` | Intervalo de envio de *heartbeats* em milissegundos |
| `HB_TIMEOUT_MS` | `--hb-timeout-ms` | `500` | Limiar de suspeita de falha em milissegundos |
| `RPC_TIMEOUT_MS` | `--rpc-timeout-ms` | `1000` | *Deadline* explícito obrigatório de todas as chamadas gRPC |

---

## 5. Como Executar o Cluster via Docker Compose v2

### 5.1. Construir e Iniciar o Cluster de 5 Nós
```bash
docker compose up -d --build
```
Os 5 contêineres (`node-1` a `node-5`) serão iniciados na rede bridge `mc714-net` e expostos localmente nas portas `50051` a `50055`.

### 5.2. Verificar Status e Logs dos Contêineres
```bash
docker compose ps
docker compose logs -f node-1
```

### 5.3. Executar a Demonstração End-to-End (Operação Normal + Falhas + Read-Repair)
Com o cluster em execução (ou iniciando um cluster local automático com `--spawn-local`), execute:
```bash
# Contra o cluster Docker Compose ativo nas portas 50051..50055:
uv run python scripts/demo_cluster.py

# Ou subindo e encerrando automaticamente os 5 nós em processos locais:
uv run python scripts/demo_cluster.py --spawn-local
```

### 5.4. Encerrar e Limpar o Cluster
```bash
docker compose down -v
```

---

## 6. Verificação de Qualidade, Tipagem e Testes Automatizados

O projeto possui verificação estrita de lint (Ruff com complexidade ciclomática McCabe $\le 10$), formatação, checagem estática de tipos (Pyright) e suíte de testes unitários e de integração com cobertura $> 90\%$:

```bash
# 1. Lint e complexidade ciclomática
uv run ruff check .

# 2. Verificação de formatação
uv run ruff format --check .

# 3. Checagem estática de tipos
uv run pyright

# 4. Testes unitários e de integração multi-nó gRPC com cobertura de código
uv run pytest --cov=mc714_p2 --cov-report=term-missing
```

---

## 7. Reprodução Completa dos Experimentos e Gráficos

Para reexecutar os **3 experimentos** (com descarte de *warm-up*, múltiplas réplicas com sementes registradas, cenários normais e de falha) e regenerar automaticamente os dados brutos em `results/raw/experiments_raw.json`, as tabelas estatísticas em `results/tables/*.csv` e os gráficos vetoriais em `results/figures/*.pdf`:

```bash
uv run python scripts/run_experiments.py \
  --replicas 5 \
  --warmup-ops 3 \
  --measured-ops 12 \
  --output-dir results
```

### Experimentos Executados:
1. **`exp1_mutex_scalability`**: Avalia a Exclusão Mútua de Ricart-Agrawala variando o tamanho do cluster $N \in \{3, 5, 7\}$ e o atraso sintético de rede $\in \{0, 5, 15\}\text{ ms}$, comprovando o crescimento linear de mensagens $2(N - 1)$, zero violações de *Safety* e o impacto na latência (média, DP, p50, p95, p99) e vazão (ops/s).
2. **`exp2_quorum_tradeoffs`**: Avalia o Quorum de Gifford em $N = 5$ para três políticas $(N_R, N_W) \in \{(3,3), (2,4), (1,5)\}$ sob operação normal e com falha de 1 réplica + recuperação com *Read-Repair*, medindo os *trade-offs* de latência de leitura/escrita e garantindo 100% de leituras consistentes.
3. **`exp3_heartbeat_detection`**: Avalia o Detector de Falhas por *Heartbeat* integrado ao Ricart-Agrawala variando $(\Delta t_{\text{hb}}, T_{\text{suspect}}) \in \{(50, 150), (100, 300), (200, 600)\}\text{ ms}$ durante *crash-stop* de um nó par, medindo o tempo de detecção, o tempo de recuperação da seção crítica (*Liveness*) e a taxa de falsos positivos sob jitter de rede.
