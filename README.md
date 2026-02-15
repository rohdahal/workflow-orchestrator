# Workflow Orchestrator

Spring Boot + Postgres + Spark workflow orchestrator for NYC TLC batch pipelines.

## Supported DAGs

- `daily_tip_insights`
- `monthly_revenue_rollup`
- `driver_activity_summary`
- `all` (runs all three and produces a 3-section PDF report)

## Supported datasets

- `yellow`
- `green`
- `fhv`
- `hvfhv`

## Architecture flow

1. API creates a `dag_run` and queued `task_run` rows in Postgres.
2. Worker (`SPRING_PROFILES_ACTIVE=worker`) claims queued tasks.
3. Worker executes Spark/SQL task logic for the selected DAG + dataset/date range.
4. Task results update `task_run`; DAG status is recomputed in `dag_run`.
5. On DAG success, report generation creates a PDF and uploads to S3.
6. Report metadata and upload result are persisted in `dag_report`.

## Prerequisites

- Java 17+
- Docker + Docker Compose
- AWS credentials with S3 read/write access

## Environment

Create `.env` in project root:

You may Use `.env.example` as reference for required keys.

```env
AWS_REGION=YOUR_REGION
AWS_ACCESS_KEY_ID=YOUR_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET

ORCH_S3_BUCKET=your-bucket
ORCH_S3_PREFIX=workflow-orchestrator
```

Notes:
- Reports upload to `s3://$ORCH_S3_BUCKET/$ORCH_S3_PREFIX/reports/`
- Spark + worker also read this `.env`

Minimum IAM permissions for your AWS principal:
- `s3:ListBucket` on the target bucket
- `s3:GetObject` on `s3://$ORCH_S3_BUCKET/$ORCH_S3_PREFIX/*`
- `s3:PutObject` on `s3://$ORCH_S3_BUCKET/$ORCH_S3_PREFIX/*`

## Run locally

1. Start infra:

```bash
docker compose up -d
```

2. Start API (terminal A):

```bash
./mvnw spring-boot:run
```

3. Start worker (terminal B):

```bash
SPRING_PROFILES_ACTIVE=worker ./mvnw spring-boot:run
```

## Trigger DAG runs (February 2025)

```bash
curl -sS -X POST "http://localhost:8080/api/dags/daily_tip_insights/yellow/runs?startDate=2025-02-01&endDate=2025-02-28"

curl -sS -X POST "http://localhost:8080/api/dags/monthly_revenue_rollup/yellow/runs?startDate=2025-02-01&endDate=2025-02-28"

curl -sS -X POST "http://localhost:8080/api/dags/driver_activity_summary/yellow/runs?startDate=2025-02-01&endDate=2025-02-28"

curl -sS -X POST "http://localhost:8080/api/dags/all/yellow/runs?startDate=2025-02-01&endDate=2025-02-28"
```

## Track execution

```bash
docker exec -i orch-postgres psql -U orchestrator -d orchestrator -c "select id,dag_id,dataset,status,started_at,finished_at from dag_run order by id desc limit 10;"

docker exec -i orch-postgres psql -U orchestrator -d orchestrator -c "select id,dag_run_id,task_id,status,error from task_run order by id desc limit 20;"

docker exec -i orch-postgres psql -U orchestrator -d orchestrator -c "select dag_run_id,status,report_key,uploaded_at,error from dag_report order by id desc limit 20;"
```

## Verify report in S3

```bash
aws s3 ls "s3://$ORCH_S3_BUCKET/$ORCH_S3_PREFIX/reports/" --recursive | tail -n 20
```

## First successful run checklist

- `dag_run.status = success`
- all rows in `task_run` for that `dag_run_id` are `success`
- `dag_report.status = success`
- `dag_report.report_key` exists in S3 under `reports/`

## Expected S3 layout

- Data/input prefix: `s3://$ORCH_S3_BUCKET/$ORCH_S3_PREFIX/...`
- Reports prefix: `s3://$ORCH_S3_BUCKET/$ORCH_S3_PREFIX/reports/dag-report-*.pdf`

## Clean restart

```bash
pkill -f 'WorkflowOrchestratorApplication' || true
docker compose down -v
docker compose up -d
./mvnw spring-boot:run
# In another terminal:
SPRING_PROFILES_ACTIVE=worker ./mvnw spring-boot:run
```

## License

UNLICENSED
