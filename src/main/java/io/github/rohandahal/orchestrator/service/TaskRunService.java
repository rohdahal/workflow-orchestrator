package io.github.rohandahal.orchestrator.service;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Locale;

@Service
public class TaskRunService {

  private final JdbcTemplate jdbc;

  public TaskRunService(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public record TaskRunResult(
      long taskRunId,
      long dagRunId,
      String taskId,
      String status,
      int attempt,
      OffsetDateTime startedAt,
      OffsetDateTime finishedAt,
      String error
  ) {}

  @Transactional
  public TaskRunResult start(long taskRunId) {
    // 1) Load the current task_run row.
    var row = fetchTaskRun(taskRunId);

    // 2) Enforce the allowed state transition.
    String current = row.status;
    if (!"queued".equals(current)) {
      throw new IllegalStateException("TaskRun " + taskRunId + " cannot start from status=" + current);
    }

    // 3) Mark the task as running and set started_at.
    jdbc.update(
        "update task_run set status = ?, started_at = now() where id = ?",
        "running",
        taskRunId
    );

    // 4) Best-effort: mark the dag_run as running when the first task starts.
    jdbc.update(
        "update dag_run set status = ?, started_at = coalesce(started_at, now()) where id = ? and status = ?",
        "running",
        row.dagRunId,
        "queued"
    );

    // 5) Return the refreshed row after updates.
    return fetchTaskRun(taskRunId);
  }

  @Transactional
  public TaskRunResult finish(long taskRunId, String status, String error) {
    // 1) Load the current task_run row.
    var row = fetchTaskRun(taskRunId);

    // 2) Enforce the allowed state transition.
    String current = row.status;
    if (!"running".equals(current)) {
      throw new IllegalStateException("TaskRun " + taskRunId + " cannot finish from status=" + current);
    }

    // 3) Normalize the requested finish status and error message.
    String normalized = normalizeFinishStatus(status);
    String err = ("failed".equals(normalized)) ? (error == null ? "failed" : error) : null;

    // 4) Mark the task as finished and set finished_at.
    jdbc.update(
        "update task_run set status = ?, finished_at = now(), error = ? where id = ?",
        normalized,
        err,
        taskRunId
    );

    // 5) Update the parent dag_run status based on all task_runs.
    recomputeDagRunStatus(row.dagRunId);

    // 6) Return the refreshed row after updates.
    return fetchTaskRun(taskRunId);
  }

  private String normalizeFinishStatus(String status) {
    if (status == null) {
      throw new IllegalArgumentException("status is required");
    }
    String s = status.trim().toLowerCase(Locale.ROOT);
    return switch (s) {
      case "success", "succeeded", "done" -> "success";
      case "failed", "fail", "error" -> "failed";
      default -> throw new IllegalArgumentException("Invalid finish status: " + status + ". Use success|failed.");
    };
  }

  private void recomputeDagRunStatus(long dagRunId) {
    // 1) If any task_run failed, mark the dag_run failed.
    Integer failedCount = jdbc.queryForObject(
        "select count(*) from task_run where dag_run_id = ? and status = ?",
        Integer.class,
        dagRunId,
        "failed"
    );
    if (failedCount != null && failedCount > 0) {
      jdbc.update(
          "update dag_run set status = ?, finished_at = now() where id = ?",
          "failed",
          dagRunId
      );
      return;
    }

    // 2) If all task_runs succeeded, mark the dag_run successful.
    Integer total = jdbc.queryForObject(
        "select count(*) from task_run where dag_run_id = ?",
        Integer.class,
        dagRunId
    );
    Integer success = jdbc.queryForObject(
        "select count(*) from task_run where dag_run_id = ? and status = ?",
        Integer.class,
        dagRunId,
        "success"
    );

    if (total != null && total > 0 && success != null && success.equals(total)) {
      jdbc.update(
          "update dag_run set status = ?, finished_at = now() where id = ?",
          "success",
          dagRunId
      );
    }
  }

  private TaskRunResult fetchTaskRun(long taskRunId) {
    try {
      return jdbc.queryForObject(
          "select id, dag_run_id, task_id, status, attempt, started_at, finished_at, error from task_run where id = ?",
          (rs, rowNum) -> new TaskRunResult(
              rs.getLong("id"),
              rs.getLong("dag_run_id"),
              rs.getString("task_id"),
              rs.getString("status"),
              rs.getInt("attempt"),
              rs.getObject("started_at", OffsetDateTime.class),
              rs.getObject("finished_at", OffsetDateTime.class),
              rs.getString("error")
          ),
          taskRunId
      );
    } catch (EmptyResultDataAccessException e) {
      throw new IllegalArgumentException("TaskRun not found: " + taskRunId);
    }
  }
}
