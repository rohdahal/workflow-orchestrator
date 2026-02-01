package io.github.rohandahal.orchestrator.worker;

import io.github.rohandahal.orchestrator.service.TaskContextService;
import io.github.rohandahal.orchestrator.service.TaskRunService;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@Profile("worker")
public class TaskWorker implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);

  private final JdbcTemplate jdbc;
  private final TaskRunService taskRunService;
  private final TaskContextService ctx;
  private final TransactionTemplate tx;

  public TaskWorker(JdbcTemplate jdbc, TaskRunService taskRunService, TaskContextService ctx, PlatformTransactionManager tm) {
    this.jdbc = jdbc;
    this.taskRunService = taskRunService;
    this.ctx = ctx;
    this.tx = new TransactionTemplate(tm);
  }

  @Override
  public void run(ApplicationArguments args) {
    // Single-thread worker loop
    while (true) {
      // Claim the next queued task_run row
      Long taskRunId = claimNextQueuedTask();
      if (taskRunId == null) {
        // No work available.
        log.debug("No queued tasks found.");
        sleepQuietly(750);
        continue;
      }

      log.info("Claimed task_run id={}", taskRunId);
      try {
        // Load metadata needed to execute (task_id, run_date, dag_run_id).
        var context = ctx.getContext(taskRunId);

        log.info(
            "Executing task_run id={} dag_id={} run_id={} task_id={} attempt={}",
            context.taskRunId(),
            context.dagId(),
            context.runId(),
            context.taskId(),
            context.attempt()
        );
        // Execute the task 
        executeTask(context);

        // Mark the task successful and let TaskRunService update the parent DAG.
        taskRunService.finish(taskRunId, "success", null);
        log.info("Finished task_run id={} status=success", taskRunId);
      } catch (Exception e) {
        try {
          // Mark the task failed and store the error.
          taskRunService.finish(taskRunId, "failed", e.getMessage());
          log.error("Finished task_run id={} status=failed error={}", taskRunId, e.getMessage(), e);
        } catch (Exception ignored) {
          // Ignore secondary failure.
          log.error("Secondary failure while finishing task_run id={}", taskRunId, ignored);
        }
      }
    }
  }

  private void executeTask(TaskContextService.TaskContext context) {
    // TODO: Route to a real executor based on context.taskId().
    sleepQuietly(250);
  }

  private Long claimNextQueuedTask() {
    return tx.execute(status -> {
      // Claim one queued row without blocking other workers.
      Long id = jdbc.query(
          """
          select id
          from task_run
          where status = 'queued'
          order by id
          for update skip locked
          limit 1
          """,
          rs -> rs.next() ? rs.getLong("id") : null
      );

      if (id == null) {
        return null;
      }

      // Transition queued -> running and set started_at.
      taskRunService.start(id);

      return id;
    });
  }

  private void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
