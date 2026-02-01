package io.github.rohandahal.orchestrator.api;

import io.github.rohandahal.orchestrator.service.DagRunService;
import io.github.rohandahal.orchestrator.service.DagRunService.CreateRunResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;

@RestController
@RequestMapping("/api/dags")
public class DagRunController {

  private final DagRunService dagRunService;

  public DagRunController(DagRunService dagRunService) {
    this.dagRunService = dagRunService;
  }

  @PostMapping("/{dagId}/runs")
  public ResponseEntity<CreateRunResult> createRun(
      @PathVariable String dagId,
      @RequestParam("date") LocalDate date
  ) {
    return ResponseEntity.ok(dagRunService.createDagRun(dagId, date));
  }
}