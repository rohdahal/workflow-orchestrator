package io.github.rohandahal.orchestrator.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
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
      String dataset,
      String runId,
      LocalDate startDate,
      LocalDate endDate,
      String status,
      long taskRunDbId,
      String taskId,
      int attempt
  ) {}

  public record DagRunContext(
      long dagRunDbId,
      String dagId,
      String dataset,
      String runId,
      LocalDate runDate,
      LocalDate startDate,
      LocalDate endDate,
      String status
  ) {}

  @Value("${orchestrator.spark.enabled:false}")
  private boolean sparkEnabled;

  @Value("${orchestrator.spark.submitCommand:}")
  private String sparkSubmitCommand;

  @Value("${orchestrator.spark.dailyCountMainClass:io.github.rohandahal.spark.TlcDailyCountJob}")
  private String dailyCountMainClass;

  public DagRunContext getDagRunContext(long dagRunId) {
    SqlRowSet rs = jdbc.queryForRowSet("""
        select id, dag_id, dataset, run_id, run_date, start_date, end_date, status
        from dag_run
        where id = ?
        """, dagRunId);

    if (!rs.next()) {
      throw new IllegalArgumentException("Unknown dag_run id: " + dagRunId);
    }

    return new DagRunContext(
        rs.getLong("id"),
        rs.getString("dag_id"),
        rs.getString("dataset"),
        rs.getString("run_id"),
        rs.getDate("run_date").toLocalDate(),
        rs.getDate("start_date").toLocalDate(),
        rs.getDate("end_date").toLocalDate(),
        rs.getString("status")
    );
  }

  public boolean isDailyCountsAvailable(String dataset, LocalDate startDate, LocalDate endDate) {
    Integer found = jdbc.queryForObject("""
        select 1
        from tlc_daily_counts
        where dataset = ?
          and pickup_date >= ?
          and pickup_date <= ?
        limit 1
        """, Integer.class, dataset, startDate, endDate);

    return found != null;
  }

  public void loadDailyCountsWithSpark(DagRunContext ctx) {
    if (!sparkEnabled || sparkSubmitCommand == null || sparkSubmitCommand.isBlank()) {
      throw new IllegalStateException(
          "Daily counts are missing, but Spark execution is not configured. " +
          "Set orchestrator.spark.enabled=true and orchestrator.spark.submitCommand, " +
          "and provide env vars ORCH_SPARK_JAR, ORCH_TLC_S3_PREFIX, ORCH_JDBC_URL, ORCH_JDBC_USER, ORCH_JDBC_PASSWORD."
      );
    }

    String jarPath = System.getenv("ORCH_SPARK_JAR");
    String s3Prefix = System.getenv("ORCH_TLC_S3_PREFIX");
    String jdbcUrl = System.getenv("ORCH_JDBC_URL");
    String jdbcUser = System.getenv("ORCH_JDBC_USER");
    String jdbcPassword = System.getenv("ORCH_JDBC_PASSWORD");

    if (isBlank(jarPath) || isBlank(s3Prefix) || isBlank(jdbcUrl) || isBlank(jdbcUser) || isBlank(jdbcPassword)) {
      throw new IllegalStateException(
          "Spark execution is enabled, but required env vars are missing. " +
          "Need ORCH_SPARK_JAR, ORCH_TLC_S3_PREFIX, ORCH_JDBC_URL, ORCH_JDBC_USER, ORCH_JDBC_PASSWORD."
      );
    }

    // Use the monthly anchor (run_date) to decide which TLC parquet file to load.
    String yearMonth = ctx.runDate().toString().substring(0, 7);

    ProcessBuilder pb = new ProcessBuilder(
        sparkSubmitCommand,
        "--class", dailyCountMainClass,
        jarPath,
        "--dataset", ctx.dataset(),
        "--year_month", yearMonth,
        "--s3_path", s3Prefix,
        "--jdbc_url", jdbcUrl,
        "--jdbc_user", jdbcUser,
        "--jdbc_password", jdbcPassword
    );

    pb.inheritIO();

    try {
      Process p = pb.start();
      int code = p.waitFor();
      if (code != 0) {
        throw new IllegalStateException("spark-submit failed with exit code: " + code);
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("spark-submit interrupted", ie);
    } catch (Exception e) {
      throw new IllegalStateException("spark-submit failed", e);
    }

    // Re-check to confirm data is now present.
    if (!isDailyCountsAvailable(ctx.dataset(), ctx.startDate(), ctx.endDate())) {
      throw new IllegalStateException("Spark job completed but daily counts are still missing for the requested range");
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  public void markDagFailed(long dagRunId) {
    jdbc.update("""
        update dag_run
        set status = 'failed', finished_at = now()
        where id = ?
        """, dagRunId);
  }

  public void updateDagStatusIfComplete(long dagRunId) {
    Integer remaining = jdbc.queryForObject("""
        select count(*)
        from task_run
        where dag_run_id = ?
          and status in ('queued', 'running')
        """, Integer.class, dagRunId);

    if (remaining != null && remaining > 0) {
      return;
    }

    Integer failed = jdbc.queryForObject("""
        select count(*)
        from task_run
        where dag_run_id = ?
          and status = 'failed'
        """, Integer.class, dagRunId);

    String finalStatus = (failed != null && failed > 0) ? "failed" : "success";

    jdbc.update("""
        update dag_run
        set status = ?, finished_at = now()
        where id = ?
        """, finalStatus, dagRunId);
  }

  @Transactional
  public CreateRunResult createDagRun(String dagId, String dataset, LocalDate startDate, LocalDate endDate) {

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

    // Monthly dataset anchor: run_date always uses the first day of the start month (YYYY-MM-01).
    LocalDate runDate = startDate.withDayOfMonth(1);
    // 3) Create a new DAG execution record for the requested date range.
    String runId = dagId + ":" + dataset + ":" + startDate + ":" + endDate + ":" + UUID.randomUUID();

    long dagRunId = jdbc.queryForObject("""
        insert into dag_run(
          dag_id,
          run_id,
          run_date,
          dataset,
          start_date,
          end_date,
          status,
          started_at,
          finished_at
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        returning id
        """,
        Long.class,
        dagId,
        runId,
        runDate, 
        dataset,
        startDate,
        endDate,
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
        dagRunId, dagId, dataset, runId, startDate, endDate, "queued",
        taskRunId, taskId, 1
    );
  }
}