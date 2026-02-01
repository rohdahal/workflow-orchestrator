package io.github.rohandahal.orchestrator.api;

import io.github.rohandahal.orchestrator.service.DagRunService;
import io.github.rohandahal.orchestrator.service.DagRunService.CreateRunResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
/**
 * Predefined DAGs owned by the system (clients do not invent dagIds):
 * - daily_tip_insights
 * - monthly_revenue_rollup
 * - driver_activity_summary
 * - all (composite / umbrella workflows)
 */
/**
 * Supported TLC taxi datasets (execution parameter, not a DAG):
 * - yellow
 * - green
 * - fhv
 * - hvfhv
 */
@RestController
@RequestMapping("/api/dags")
public class DagRunController {

  private final DagRunService dagRunService;

  public DagRunController(DagRunService dagRunService) {
    this.dagRunService = dagRunService;
  }

  private static final java.util.Set<String> SUPPORTED_DATASETS = java.util.Set.of(
      "yellow", "green", "fhv", "hvfhv"
  );

  @PostMapping("/{dagId}/{dataset}/runs")
  public ResponseEntity<CreateRunResult> createRun(
      @PathVariable String dagId,
      @PathVariable String dataset,
      @RequestParam("startDate") LocalDate startDate,
      @RequestParam("endDate") LocalDate endDate
  ) {
      if (!SUPPORTED_DATASETS.contains(dataset)) {
          throw new IllegalArgumentException("Unsupported dataset: " + dataset);
      }
      if (startDate.isAfter(endDate)) {
          throw new IllegalArgumentException("startDate must be on or before endDate");
      }

      return ResponseEntity.ok(dagRunService.createDagRun(dagId, dataset, startDate, endDate));
  }
}