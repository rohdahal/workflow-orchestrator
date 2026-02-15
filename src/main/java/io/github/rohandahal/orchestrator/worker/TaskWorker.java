package io.github.rohandahal.orchestrator.worker;

import io.github.rohandahal.orchestrator.service.DagReportService;
import io.github.rohandahal.orchestrator.service.DagRunService;
import io.github.rohandahal.orchestrator.service.TaskRunService;
import io.github.rohandahal.orchestrator.executor.IngestTlcMonthExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Background worker that polls for queued task runs and executes them.
 */
@Profile("worker")
@Component
public class TaskWorker {

  private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);

  private final JdbcTemplate jdbc;
  private final TaskRunService taskRunService;
  private final DagRunService dagRunService;
  private final DagReportService dagReportService;
  private final IngestTlcMonthExecutor ingestTlcMonthExecutor;

  public TaskWorker(
      JdbcTemplate jdbc,
      TaskRunService taskRunService,
      DagRunService dagRunService,
      DagReportService dagReportService,
      IngestTlcMonthExecutor ingestTlcMonthExecutor
  ) {
    this.jdbc = jdbc;
    this.taskRunService = taskRunService;
    this.dagRunService = dagRunService;
    this.dagReportService = dagReportService;
    this.ingestTlcMonthExecutor = ingestTlcMonthExecutor;
  }

  /**
   * Polls the database for the next queued task and executes it.
   */
  @Scheduled(fixedDelay = 5000)
  public void pollAndExecute() {
    ClaimedTaskRun claimed = claimNextQueuedTaskRun();
    if (claimed == null) {
      return;
    }

    log.info("Claimed task_run id={} dag_run_id={} task_id={}", claimed.taskRunId(), claimed.dagRunId(), claimed.taskId());
    try {
      executeTask(claimed.dagRunId(), claimed.taskId());
      TaskRunService.TaskRunResult finished = taskRunService.finish(claimed.taskRunId(), "success", null);
      log.info("Finished task_run id={} status=success", claimed.taskRunId());

      if (dagRunService.isDagRunSuccessful(finished.dagRunId())) {
        dagReportService.uploadIfDagSucceeded(finished.dagRunId());
        log.info("Dag run id={} is successful; report upload attempted", finished.dagRunId());
      }

    } catch (Exception e) {
      try {
        taskRunService.finish(claimed.taskRunId(), "failed", e.getMessage());
      } catch (Exception ignore) {
        // // ignore to avoid masking the original error
      }

      dagRunService.markDagFailed(claimed.dagRunId());
      log.error("Task run id={} failed: {}", claimed.taskRunId(), e.getMessage(), e);
    }
  }

  private void executeTask(long dagRunId, String taskId) {
    DagRunService.DagRunContext ctx = dagRunService.getDagRunContext(dagRunId);
    String yearMonth = ctx.runDate().toString().substring(0, 7);

    switch (taskId) {
      case "tip_behavior" -> {
        ingestTlcMonthExecutor.ingest(ctx.dataset(), yearMonth, false);
        dagRunService.ensureDailyCountsAvailable(ctx);
      }
      case "revenue_rollup" -> {
        ingestTlcMonthExecutor.ingest(ctx.dataset(), yearMonth, false);
        dagRunService.ensureDailyCountsAvailable(ctx);
      }
      case "driver_activity" -> {
        ingestTlcMonthExecutor.ingest(ctx.dataset(), yearMonth, false);
        dagRunService.ensureDailyCountsAvailable(ctx);
      }
      default -> throw new IllegalArgumentException("Unsupported taskId: " + taskId);
    }
  }

  private record ClaimedTaskRun(long taskRunId, long dagRunId, String taskId) {}

  private ClaimedTaskRun claimNextQueuedTaskRun() {
    List<Map<String, Object>> rows = jdbc.queryForList("""
        with next_task as (
          select id
          from task_run
          where status = 'queued'
          order by id asc
          for update skip locked
          limit 1
        )
        update task_run tr
        set status = 'running',
            started_at = now()
        from next_task nt
        where tr.id = nt.id
        returning tr.id, tr.dag_run_id, tr.task_id
        """);

    if (rows.isEmpty()) {
      return null;
    }

    long taskRunId = ((Number) rows.get(0).get("id")).longValue();
    long dagRunId = ((Number) rows.get(0).get("dag_run_id")).longValue();
    String taskId = (String) rows.get(0).get("task_id");

    jdbc.update("""
        update dag_run
        set status = ?, started_at = coalesce(started_at, now())
        where id = ? and status = ?
        """, "running", dagRunId, "queued");

    return new ClaimedTaskRun(taskRunId, dagRunId, taskId);
  }
}
