package io.github.rohandahal.orchestrator.api;

import io.github.rohandahal.orchestrator.service.TaskRunService;
import io.github.rohandahal.orchestrator.service.TaskRunService.TaskRunResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/task-runs")
public class TaskRunController {

  private final TaskRunService taskRunService;

  public TaskRunController(TaskRunService taskRunService) {
    this.taskRunService = taskRunService;
  }

  @PostMapping("/{taskRunId}/start")
  public ResponseEntity<TaskRunResult> start(@PathVariable long taskRunId) {
    return ResponseEntity.ok(taskRunService.start(taskRunId));
  }

  @PostMapping("/{taskRunId}/finish")
  public ResponseEntity<TaskRunResult> finish(
      @PathVariable long taskRunId,
      @RequestParam("status") String status,
      @RequestParam(value = "error", required = false) String error
  ) {
    return ResponseEntity.ok(taskRunService.finish(taskRunId, status, error));
  }
}
