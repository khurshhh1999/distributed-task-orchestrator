# Distributed Task Orchestration Platform

A fault-tolerant distributed task orchestration platform built with **Java**, **Spring Boot**, **PostgreSQL**, **Apache Kafka**, and **Redis**. Designed to handle **50,000+ asynchronous jobs daily** with retry backoff, idempotency keys, dead-letter queue processing, and execution metrics.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│  task-api   │────▶│  Kafka       │────▶│  task-worker    │
│  (REST API) │     │  task-queue  │     │  (consumers)    │
└──────┬──────┘     └──────┬───────┘     └────────┬────────┘
       │                   │                      │
       │            ┌──────┴───────┐              │
       │            │ task-retry   │◀─────────────┘
       │            │ task-dlq     │
       ▼            └──────────────┘
┌─────────────┐        ▲
│ PostgreSQL  │  Task state, retry counts, status
└─────────────┘        │
       ▲               │  Retry Sweeper (scheduled): reclaims tasks stuck
       │               └─ in RETRY_SCHEDULED past due and re-queues them,
       │                  so retries survive broker/worker failures
┌─────────────┐
│   Redis     │  Idempotency key deduplication
└─────────────┘
```

### Retry durability

Retries are normally driven by the Kafka `task-retry` topic. As a safety net, the
worker runs a **scheduled retry sweeper** that periodically scans PostgreSQL for
tasks stranded in `RETRY_SCHEDULED` past their due time (a Kafka message was lost, a
broker was down, or a worker crashed mid-retry) and re-queues them to `task-queue`.
Each reclaim is guarded by an optimistic-lock `version` column, so running multiple
worker replicas never recovers the same task twice.

### Microservices

| Service | Port | Responsibility |
|---------|------|----------------|
| **task-api** | 8080 | Submit tasks, query status, metrics summary |
| **task-worker** | 8081 | Consume Kafka, execute tasks, retry/DLQ handling |

## Key Features

- **Idempotency keys** — Redis-backed deduplication prevents duplicate job processing on retries
- **Exponential retry backoff** — 1s → 2s → 4s → 8s → 16s (capped at 60s), up to 5 retries. Scheduled retries are never executed before their due time (the retry consumer re-queues instead of running early)
- **Dead-letter queue (DLQ)** — Failed tasks after max retries are published to `task-dlq` and persisted with status `DEAD_LETTERED` for recovery visibility
- **DLQ replay** — Reset and re-queue dead-lettered tasks individually or in bulk via the API, so recovered failures can be reprocessed without duplicating work
- **Durable retry recovery** — A scheduled sweeper reclaims retries stranded in the database (from lost Kafka messages or crashed workers) and re-queues them, with optimistic locking to stay safe across multiple worker replicas
- **Execution metrics** — Prometheus counters and timers via Spring Actuator (submitted, idempotent hits, retries, recovered retries, DLQ, replays, execution duration)
- **Fault tolerance** — Manual Kafka acks, transactional DB updates, idempotent producers on both API and worker, and reservation cleanup on failed submissions

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.9+
- Docker & Docker Compose

### Run with Docker Compose

```bash
cd distributed-task-orchestrator
docker compose up --build
```

This starts PostgreSQL, Redis, Kafka, task-api, and task-worker.

### Submit a Task

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-12345-email" \
  -d '{
    "taskType": "EMAIL_NOTIFICATION",
    "payload": "send-welcome-email",
    "metadata": {"userId": "42", "template": "welcome"}
  }'
```

### Check Task Status

```bash
curl http://localhost:8080/api/v1/tasks/{taskId}
```

### View Metrics

```bash
# Prometheus metrics
curl http://localhost:8080/actuator/prometheus

# Status summary
curl http://localhost:8080/api/v1/tasks/metrics/summary
```

### Simulate Failures

```bash
# Transient failure (70% chance, will retry with backoff)
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: transient-test-1" \
  -d '{"taskType":"DATA_EXPORT","payload":"transient-fail"}'

# Permanent failure (moves to DLQ after max retries)
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: dlq-test-1" \
  -d '{"taskType":"WEBHOOK_CALLBACK","payload":"force-fail"}'
```

## Observability

`docker compose up` also starts a full metrics stack:

| Tool | URL | Purpose |
|------|-----|---------|
| **Prometheus** | http://localhost:9090 | Scrapes `task-api` and `task-worker` `/actuator/prometheus`; evaluates alert rules |
| **Grafana** | http://localhost:3000 (admin / admin) | Pre-provisioned **Task Orchestrator** dashboard |
| **Alertmanager** | http://localhost:9093 | Routes fired alerts |

The Grafana dashboard is auto-loaded and visualizes submission throughput,
processing outcomes (success / retry / DLQ), sweeper recoveries, DLQ replays, and
task-execution latency (p50/p95/p99 via histogram buckets).

Prometheus alert rules (`monitoring/prometheus/alerts.yml`) cover target-down,
dead-letter growth, retry-recovery spikes (Kafka retry path unhealthy), and p95
execution-latency SLO breaches. To send notifications, add a `slack_configs` or
`webhook_configs` receiver in `monitoring/alertmanager/alertmanager.yml`.

## Local Development

```bash
# Start infrastructure only
docker compose up -d postgres redis zookeeper kafka

# Run API (from project root after install)
mvn install -DskipTests
java -jar task-api/target/task-api-1.0.0-SNAPSHOT.jar

# Run worker (separate terminal)
java -jar task-worker/target/task-worker-1.0.0-SNAPSHOT.jar
```

Copy `.env.example` to `.env` and adjust connection settings as needed.

## CI/CD

GitHub Actions workflow (`.github/workflows/ci-cd.yml`) runs:

1. **build-and-test** — Unit tests with PostgreSQL and Redis service containers
2. **integration-test** — Full stack via Docker Compose, submits a task, verifies metrics
3. **docker-publish** — Builds and pushes images on `main` (requires registry secrets)

### Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `DATABASE_USERNAME` | PostgreSQL username (defaults to `orchestrator`) |
| `DATABASE_PASSWORD` | PostgreSQL password (defaults to `orchestrator`) |
| `DOCKER_USERNAME` | Docker Hub username (optional, for image publish) |
| `DOCKER_PASSWORD` | Docker Hub password/token (optional) |

## API Reference

### `POST /api/v1/tasks`

Submit an async task. Requires `Idempotency-Key` header.

**Request body:**
```json
{
  "taskType": "EMAIL_NOTIFICATION | DATA_EXPORT | WEBHOOK_CALLBACK | REPORT_GENERATION",
  "payload": "string",
  "metadata": { "key": "value" }
}
```

**Response:** `202 Accepted` with task details and `QUEUED` status.

### `GET /api/v1/tasks/{taskId}`

Returns current task state including retry count and last error.

### `GET /api/v1/tasks?status=FAILED`

List tasks filtered by status.

### `GET /api/v1/tasks/metrics/summary`

Returns task counts grouped by status.

### `POST /api/v1/tasks/{taskId}/replay`

Replays a single dead-lettered task: resets its retry count and error, sets status back to `QUEUED`, and re-publishes it to `task-queue`. Returns `409 Conflict` if the task is not in `DEAD_LETTERED` state.

### `POST /api/v1/tasks/dlq/replay`

Bulk-replays **all** dead-lettered tasks. Returns `202 Accepted` with the count and list of re-queued tasks.

```bash
# Replay one task
curl -X POST http://localhost:8080/api/v1/tasks/{taskId}/replay

# Replay every dead-lettered task
curl -X POST http://localhost:8080/api/v1/tasks/dlq/replay
```

## Kafka Topics

| Topic | Purpose |
|-------|---------|
| `task-queue` | New task submissions |
| `task-retry` | Tasks scheduled for retry with backoff |
| `task-dlq` | Dead-letter queue for permanently failed tasks |

## Project Structure

```
distributed-task-orchestrator/
├── common/                 # Shared DTOs, enums, retry policy
├── task-api/               # REST API microservice
├── task-worker/            # Kafka consumer microservice
├── docker-compose.yml
├── .github/workflows/      # CI/CD pipeline
└── README.md
```

## Tech Stack

- **Java 17** / **Spring Boot 3.2**
- **PostgreSQL 16** — Task persistence with Flyway migrations
- **Apache Kafka** — Async message queue with retry/DLQ topics
- **Redis 7** — Idempotency key storage
- **Micrometer + Prometheus** — Execution metrics and observability

## License

MIT
