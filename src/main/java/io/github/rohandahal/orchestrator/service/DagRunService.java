package io.github.rohandahal.orchestrator.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

@Service
public class DagRunService {

  private final JdbcTemplate jdbc;

  public DagRunService(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public record CreateRunResult(
      long dagRunDbId,
      String dagId,
      String runId,
      LocalDate runDate,
      String status,
      long taskRunDbId,
      String taskId,
      int attempt
  ) {}

  @Transactional
  public CreateRunResult createDagRun(String dagId, LocalDate runDate) {

    // 1) Ensure the DAG exists (idempotent).
    jdbc.update("""
        insert into dag(dag_id, description, is_active)
        values (?, ?, true)
        on conflict (dag_id) do nothing
        """,
        dagId,
        "Created on demand by API"
    );

    // 2) Ensure the task exists for this DAG (idempotent).
    String taskId = "tip_behavior";
    jdbc.update("""
        insert into task(dag_id, task_id, task_type)
        values (?, ?, ?)
        on conflict (dag_id, task_id) do nothing
        """,
        dagId, taskId, "spark_job"
    );

    // 3) Create a new DAG execution record.
    String runId = dagId + ":" + runDate + ":" + UUID.randomUUID();

    long dagRunId = jdbc.queryForObject("""
        insert into dag_run(dag_id, run_id, run_date, status, started_at, finished_at)
        values (?, ?, ?, ?, ?, ?)
        returning id
        """,
        Long.class,
        dagId,
        runId,
        runDate,
        "queued",
        (OffsetDateTime) null,
        (OffsetDateTime) null
    );

    // 4) Create the first task attempt for this run.
    long taskRunId = jdbc.queryForObject("""
        insert into task_run(dag_run_id, task_id, status, attempt, started_at, finished_at, error)
        values (?, ?, ?, ?, ?, ?, ?)
        returning id
        """,
        Long.class,
        dagRunId,
        taskId,
        "queued",
        1,
        (OffsetDateTime) null,
        (OffsetDateTime) null,
        null
    );

    // 5) Return ids for tracing/debugging.
    return new CreateRunResult(
        dagRunId, dagId, runId, runDate, "queued",
        taskRunId, taskId, 1
    );
  }
}