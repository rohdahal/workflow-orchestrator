package io.github.rohandahal.orchestrator.worker;

import io.github.rohandahal.orchestrator.service.DagRunService;
import io.github.rohandahal.orchestrator.service.TaskRunService;
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

  private final JdbcTemplate jdbc;
  private final TaskRunService taskRunService;
  private final DagRunService dagRunService;

  public TaskWorker(JdbcTemplate jdbc, TaskRunService taskRunService, DagRunService dagRunService) {
    this.jdbc = jdbc;
    this.taskRunService = taskRunService;
    this.dagRunService = dagRunService;
  }

  /**
   * Polls the database for the next queued task and executes it.
   */
  @Scheduled(fixedDelay = 5000)
  public void pollAndExecute() {
    // 1) Fetch the next queued task_run id, if any.
    Long taskRunId = findNextQueuedTaskRunId();
    if (taskRunId == null) {
      return;
    }

    TaskRunService.TaskRunResult started = null;

    try {
      // 2) Transition queued -> running using the service state machine.
      started = taskRunService.start(taskRunId);

      // 3) Execute the task workload.
      executeTask(started.dagRunId());

      // 4) Transition running -> success.
      taskRunService.finish(taskRunId, "success", null);

    } catch (Exception e) {
      // 5) Transition running -> failed (best-effort) and capture the error.
      try {
        taskRunService.finish(taskRunId, "failed", e.getMessage());
      } catch (Exception ignore) {
        // ignore to avoid masking the original error
      }

      if (started != null) {
        dagRunService.markDagFailed(started.dagRunId());
      }
    }
  }

  
  private void executeTask(long dagRunId) {
    // Load execution parameters (dataset + range + monthly anchor).
    DagRunService.DagRunContext ctx = dagRunService.getDagRunContext(dagRunId);

    // Check whether aggregated data needed for reporting is already present.
    boolean ready = dagRunService.isDailyCountsAvailable(ctx.dataset(), ctx.startDate(), ctx.endDate());
    if (ready) {
      return;
    }

    // Trigger a Spark job to materialize daily aggregates for this monthly anchor.
    dagRunService.loadDailyCountsWithSpark(ctx);
  }

  // DB poll helper: select the next queued task_run id.
  private Long findNextQueuedTaskRunId() {
    List<Map<String, Object>> rows = jdbc.queryForList("""
        select id
        from task_run
        where status = 'queued'
        order by id asc
        limit 1
        """);

    if (rows.isEmpty()) {
      return null;
    }

    return ((Number) rows.get(0).get("id")).longValue();
  }
}