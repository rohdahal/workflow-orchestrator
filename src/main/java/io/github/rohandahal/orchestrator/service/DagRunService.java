package io.github.rohandahal.orchestrator.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class DagRunService {

  public static final Set<String> SUPPORTED_DAGS = Set.of(
      "daily_tip_insights",
      "monthly_revenue_rollup",
      "driver_activity_summary",
      "all"
  );

  public static final Set<String> SUPPORTED_DATASETS = Set.of("yellow", "green", "fhv", "hvfhv");

  private static final String TASK_TIP_BEHAVIOR = "tip_behavior";
  private static final String TASK_REVENUE_ROLLUP = "revenue_rollup";
  private static final String TASK_DRIVER_ACTIVITY = "driver_activity";

  private static final Map<String, List<TaskDefinition>> DAG_TASKS = Map.of(
      "daily_tip_insights", List.of(new TaskDefinition(TASK_TIP_BEHAVIOR, "spark_job")),
      "monthly_revenue_rollup", List.of(new TaskDefinition(TASK_REVENUE_ROLLUP, "sql_rollup")),
      "driver_activity_summary", List.of(new TaskDefinition(TASK_DRIVER_ACTIVITY, "sql_rollup")),
      "all", List.of(
          new TaskDefinition(TASK_TIP_BEHAVIOR, "spark_job"),
          new TaskDefinition(TASK_REVENUE_ROLLUP, "sql_rollup"),
          new TaskDefinition(TASK_DRIVER_ACTIVITY, "sql_rollup")
      )
  );

  private final JdbcTemplate jdbc;
  private final Environment env;

  public DagRunService(JdbcTemplate jdbc, Environment env) {
    this.jdbc = jdbc;
    this.env = env;
  }

  public record CreateRunResult(
      long dagRunDbId,
      String dagId,
      String dataset,
      String runId,
      LocalDate startDate,
      LocalDate endDate,
      String status,
      long firstTaskRunDbId,
      String firstTaskId,
      int attempt,
      int taskCount,
      List<String> taskIds
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

  public record TaskDefinition(String taskId, String taskType) {}

  @Value("${orchestrator.spark.enabled:${ORCHESTRATOR_SPARK_ENABLED:true}}")
  private boolean sparkEnabled;

  @Value("${orchestrator.spark.submitCommand:./scripts/spark-submit-master.sh}")
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

  public List<TaskDefinition> taskDefinitionsForDag(String dagId) {
    List<TaskDefinition> tasks = DAG_TASKS.get(dagId);
    if (tasks == null) {
      throw new IllegalArgumentException("Unsupported dagId: " + dagId);
    }
    return tasks;
  }

  public boolean isDailyCountsAvailable(String dataset, LocalDate startDate, LocalDate endDate) {
    Boolean exists = jdbc.queryForObject("""
        select exists(
          select 1
          from tlc_daily_counts
          where dataset = ?
            and pickup_date >= ?
            and pickup_date <= ?
        )
        """, Boolean.class, dataset, startDate, endDate);

    return Boolean.TRUE.equals(exists);
  }

  public void ensureDailyCountsAvailable(DagRunContext ctx) {
    boolean ready = isDailyCountsAvailable(ctx.dataset(), ctx.startDate(), ctx.endDate());
    if (ready) {
      return;
    }
    boolean resolvedSparkEnabled = isSparkEnabledResolved();
    String resolvedSubmitCommand = resolvedSparkSubmitCommand();
    if (!resolvedSparkEnabled || resolvedSubmitCommand.isBlank()) {
      throw new IllegalStateException(
          "Daily counts are missing for the requested range. " +
          "Report metrics are generated from tlc_daily_counts, not directly from parquet. " +
          "Enable Spark with orchestrator.spark.enabled=true and a valid orchestrator.spark.submitCommand."
      );
    }
    loadDailyCountsWithSpark(ctx);
  }

  public void loadDailyCountsWithSpark(DagRunContext ctx) {
    boolean resolvedSparkEnabled = isSparkEnabledResolved();
    String resolvedSubmitCommand = resolvedSparkSubmitCommand();
    if (!resolvedSparkEnabled || resolvedSubmitCommand.isBlank()) {
      throw new IllegalStateException(
          "Daily counts are missing, but Spark execution is not configured. " +
          "Set orchestrator.spark.enabled=true and orchestrator.spark.submitCommand."
      );
    }

    String jarPath = firstNonBlank(
        System.getenv("ORCH_SPARK_JAR"),
        env.getProperty("ORCH_SPARK_JAR"),
        readDotEnv("ORCH_SPARK_JAR"),
        "/opt/spark-jobs/workflow-orchestrator-0.0.1-SNAPSHOT.jar.original"
    );
    String s3Prefix = resolvedSparkS3Prefix();
    String jdbcUrl = firstNonBlank(
        System.getenv("ORCH_JDBC_URL"),
        env.getProperty("ORCH_JDBC_URL"),
        readDotEnv("ORCH_JDBC_URL"),
        "jdbc:postgresql://orch-postgres:5432/orchestrator"
    );
    String jdbcUser = firstNonBlank(
        System.getenv("ORCH_JDBC_USER"),
        env.getProperty("ORCH_JDBC_USER"),
        readDotEnv("ORCH_JDBC_USER"),
        "orchestrator"
    );
    String jdbcPassword = firstNonBlank(
        System.getenv("ORCH_JDBC_PASSWORD"),
        env.getProperty("ORCH_JDBC_PASSWORD"),
        readDotEnv("ORCH_JDBC_PASSWORD"),
        "orchestrator"
    );

    if (isBlank(s3Prefix)) {
      throw new IllegalStateException(
          "Spark execution is enabled, but S3 path is missing. Set ORCH_TLC_S3_PREFIX " +
          "or ORCH_S3_BUCKET/ORCH_S3_PREFIX."
      );
    }

    String yearMonth = ctx.runDate().toString().substring(0, 7);

    String sparkPyScript = firstNonBlank(
        System.getenv("ORCH_SPARK_PY_SCRIPT"),
        env.getProperty("ORCH_SPARK_PY_SCRIPT"),
        readDotEnv("ORCH_SPARK_PY_SCRIPT"),
        "/opt/spark-jobs/tlc_daily_count_job.py"
    );
    ProcessBuilder pb;
    if (!sparkPyScript.isBlank()) {
      pb = new ProcessBuilder(
          resolvedSubmitCommand,
          sparkPyScript,
          "--dataset", ctx.dataset(),
          "--year_month", yearMonth,
          "--s3_path", s3Prefix,
          "--jdbc_url", jdbcUrl,
          "--jdbc_user", jdbcUser,
          "--jdbc_password", jdbcPassword
      );
    } else {
      if (isBlank(jarPath)) {
        throw new IllegalStateException("Spark JAR path is missing. Set ORCH_SPARK_JAR.");
      }
      pb = new ProcessBuilder(
          resolvedSubmitCommand,
          "--class", dailyCountMainClass,
          jarPath,
          "--dataset", ctx.dataset(),
          "--year_month", yearMonth,
          "--s3_path", s3Prefix,
          "--jdbc_url", jdbcUrl,
          "--jdbc_user", jdbcUser,
          "--jdbc_password", jdbcPassword
      );
    }

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

    if (!isDailyCountsAvailable(ctx.dataset(), ctx.startDate(), ctx.endDate())) {
      throw new IllegalStateException("Spark job completed but daily counts are still missing for the requested range");
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  private boolean isSparkEnabledResolved() {
    String raw = firstNonBlank(
        System.getenv("ORCHESTRATOR_SPARK_ENABLED"),
        env.getProperty("ORCHESTRATOR_SPARK_ENABLED"),
        env.getProperty("orchestrator.spark.enabled"),
        readDotEnv("ORCHESTRATOR_SPARK_ENABLED"),
        readDotEnv("orchestrator.spark.enabled"),
        Boolean.toString(sparkEnabled)
    );
    return "true".equalsIgnoreCase(raw);
  }

  private String resolvedSparkSubmitCommand() {
    return firstNonBlank(
        sparkSubmitCommand,
        System.getenv("ORCHESTRATOR_SPARK_SUBMITCOMMAND"),
        env.getProperty("ORCHESTRATOR_SPARK_SUBMITCOMMAND"),
        env.getProperty("orchestrator.spark.submitCommand"),
        readDotEnv("ORCHESTRATOR_SPARK_SUBMITCOMMAND"),
        readDotEnv("orchestrator.spark.submitCommand"),
        "./scripts/spark-submit-master.sh"
    );
  }

  private String resolvedSparkS3Prefix() {
    String explicit = firstNonBlank(
        System.getenv("ORCH_TLC_S3_PREFIX"),
        env.getProperty("ORCH_TLC_S3_PREFIX"),
        readDotEnv("ORCH_TLC_S3_PREFIX")
    );
    if (!explicit.isBlank()) {
      return explicit;
    }
    String bucket = firstNonBlank(
        System.getenv("ORCH_S3_BUCKET"),
        env.getProperty("ORCH_S3_BUCKET"),
        readDotEnv("ORCH_S3_BUCKET")
    );
    String prefix = firstNonBlank(
        System.getenv("ORCH_S3_PREFIX"),
        env.getProperty("ORCH_S3_PREFIX"),
        readDotEnv("ORCH_S3_PREFIX")
    );
    if (bucket.isBlank()) {
      return "";
    }
    if (prefix.isBlank()) {
      return "s3a://" + bucket;
    }
    return "s3a://" + bucket + "/" + prefix;
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      if (!isBlank(value)) {
        return value.trim();
      }
    }
    return "";
  }

  private static String readDotEnv(String key) {
    try {
      Path envFile = Path.of(".env");
      if (!Files.exists(envFile)) {
        return "";
      }
      for (String line : Files.readAllLines(envFile)) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        int eq = trimmed.indexOf('=');
        if (eq <= 0) {
          continue;
        }
        String k = trimmed.substring(0, eq).trim();
        if (!key.equals(k)) {
          continue;
        }
        return trimmed.substring(eq + 1).trim();
      }
    } catch (Exception ignore) {
      // fall back to other sources
    }
    return "";
  }

  public void markDagFailed(long dagRunId) {
    jdbc.update("""
        update dag_run
        set status = 'failed', finished_at = now()
        where id = ?
        """, dagRunId);
  }

  public boolean isDagRunSuccessful(long dagRunId) {
    String status = jdbc.queryForObject("select status from dag_run where id = ?", String.class, dagRunId);
    return "success".equals(status);
  }

  @Transactional
  public CreateRunResult createDagRun(String dagId, String dataset, LocalDate startDate, LocalDate endDate) {
    if (!SUPPORTED_DAGS.contains(dagId)) {
      throw new IllegalArgumentException("Unsupported dagId: " + dagId);
    }
    if (!SUPPORTED_DATASETS.contains(dataset)) {
      throw new IllegalArgumentException("Unsupported dataset: " + dataset);
    }

    List<TaskDefinition> tasks = taskDefinitionsForDag(dagId);

    jdbc.update("""
        insert into dag(dag_id, description, is_active)
        values (?, ?, true)
        on conflict (dag_id) do nothing
        """,
        dagId,
        "System predefined DAG"
    );

    for (TaskDefinition task : tasks) {
      jdbc.update("""
          insert into task(dag_id, task_id, task_type)
          values (?, ?, ?)
          on conflict (dag_id, task_id) do nothing
          """,
          dagId, task.taskId(), task.taskType()
      );
    }

    LocalDate runDate = startDate.withDayOfMonth(1);
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

    long firstTaskRunId = -1L;
    String firstTaskId = null;

    for (int i = 0; i < tasks.size(); i++) {
      TaskDefinition task = tasks.get(i);
      long taskRunId = jdbc.queryForObject("""
          insert into task_run(dag_run_id, task_id, status, attempt, started_at, finished_at, error)
          values (?, ?, ?, ?, ?, ?, ?)
          returning id
          """,
          Long.class,
          dagRunId,
          task.taskId(),
          "queued",
          1,
          (OffsetDateTime) null,
          (OffsetDateTime) null,
          null
      );

      if (i == 0) {
        firstTaskRunId = taskRunId;
        firstTaskId = task.taskId();
      }
    }

    return new CreateRunResult(
        dagRunId,
        dagId,
        dataset,
        runId,
        startDate,
        endDate,
        "queued",
        firstTaskRunId,
        firstTaskId,
        1,
        tasks.size(),
        tasks.stream().map(TaskDefinition::taskId).toList()
    );
  }
}