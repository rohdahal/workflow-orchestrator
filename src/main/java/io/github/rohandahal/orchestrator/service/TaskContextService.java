package io.github.rohandahal.orchestrator.service;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class TaskContextService {

  private final JdbcTemplate jdbc;

  public TaskContextService(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public record TaskContext(
      long taskRunId,
      long dagRunId,
      String dagId,
      String runId,
      LocalDate runDate,
      String taskId,
      int attempt
  ) {}

  public TaskContext getContext(long taskRunId) {
    try {
      return jdbc.queryForObject(
          """
          select tr.id as task_run_id,
                 tr.task_id,
                 tr.attempt,
                 dr.id as dag_run_id,
                 dr.dag_id,
                 dr.run_id,
                 dr.run_date
          from task_run tr
          join dag_run dr on dr.id = tr.dag_run_id
          where tr.id = ?
          """,
          (rs, rowNum) -> new TaskContext(
              rs.getLong("task_run_id"),
              rs.getLong("dag_run_id"),
              rs.getString("dag_id"),
              rs.getString("run_id"),
              rs.getObject("run_date", LocalDate.class),
              rs.getString("task_id"),
              rs.getInt("attempt")
          ),
          taskRunId
      );
    } catch (EmptyResultDataAccessException e) {
      throw new IllegalArgumentException("TaskRun not found: " + taskRunId);
    }
  }
}
