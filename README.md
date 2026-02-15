# Workflow Orchestrator

A self-hosted workflow orchestrator for deadline-driven batch pipelines, inspired by real-world enterprise data platforms.

This project focuses on **orchestration mechanics** (runs, tasks, retries, audit) rather than business-specific logic. Public datasets and local infrastructure are used to simulate production-grade workflows without external dependencies.

---

## Current Status

🚧 **Work in progress**

Working today:
- Local infrastructure via Docker Compose
- PostgreSQL-backed metadata store (DAGs, runs, tasks)
- A DB-backed worker loop that claims queued tasks using `FOR UPDATE SKIP LOCKED`
- REST API to create DAG runs

Next up:
- Real task execution (NYC TLC tip behavior)
- Retries, backoff, and dead-letter handling
- Results tables + reporting endpoints

---

## Architecture

### Components

- **Spring Boot (Java 17)**
  - Orchestrator control plane
  - Creates runs and records task state transitions

- **PostgreSQL 16**
  - Durable store for:
    - workflow metadata
    - dag runs
    - task runs
    - execution history / audit
  - Also used as a simple queue for the worker (single-node friendly)

- **Docker Compose**
  - Brings up Postgres locally
  - Zero cloud dependencies
  - Reproducible environment

---

## Local Setup

### Prerequisites

- Java 17+
- Docker + Docker Compose

---

### Start Infrastructure

From the project root:

```bash
docker compose up -d
docker compose ps
```

Expected services:
- `orch-postgres` on port `5432`

---

### Verify Postgres

```bash
docker exec -it orch-postgres psql -U orchestrator -d orchestrator -c "select now();"
```

---

### Build

```bash
./mvnw clean package
```

---

### Run the API

```bash
./mvnw spring-boot:run
```

Health check:

```bash
curl http://localhost:8080/actuator/health
```

---

### Run the Worker

The worker runs in a separate process and does **not** start a web server.

```bash
SPRING_PROFILES_ACTIVE=worker ./mvnw spring-boot:run
```

---

## Quick Demo

Create a DAG run:

```bash
curl -X POST "http://localhost:8080/api/dags/daily_tip_insights/yellow/runs?startDate=2024-01-01&endDate=2024-01-31"
```

Inspect task state:

```bash
docker exec -it orch-postgres psql -U orchestrator -d orchestrator -c "select id, dag_run_id, task_id, status, started_at, finished_at from task_run order by id desc limit 5;"
```

---

## Design Principles

- **Durable and auditable execution**
  - Every run and task attempt is stored
  - Failures are first-class and will gain retries/backoff

- **Batch-first, SLA-aware execution**
  - Designed for deadline-driven batch workloads
  - Supports backfills and reprocessing by date

- **Clear separation of concerns**
  - API creates runs and persists intent
  - Worker executes tasks and updates state

---

## License

**UNLICENSED**

This project is not currently licensed for redistribution or commercial use.
