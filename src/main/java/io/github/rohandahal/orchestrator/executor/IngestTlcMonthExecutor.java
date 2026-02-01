package io.github.rohandahal.orchestrator.executor;

import io.github.rohandahal.orchestrator.service.TlcDownloader;
import io.github.rohandahal.orchestrator.service.TlcIngestManifestService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IngestTlcMonthExecutor {

  private final TlcDownloader downloader;
  private final TlcIngestManifestService manifest;

  public record IngestResult(boolean skipped, String message) {}

  public IngestTlcMonthExecutor(TlcDownloader downloader, TlcIngestManifestService manifest) {
    this.downloader = downloader;
    this.manifest = manifest;
  }

  @Transactional
  public IngestResult ingest(String dataset, String yearMonth, boolean force) {
    var dl = downloader.downloadMonth(dataset, yearMonth, force);
    var row = manifest.lockOrCreate(dataset, yearMonth, dl.s3Uri());

    if (!force && "success".equals(row.status()) && row.checksum() != null && row.checksum().equals(dl.sha256())) {
      return new IngestResult(true, "Already ingested " + dataset + " " + yearMonth);
    }

    try {
      long rowsLoaded = 0L;

      manifest.markSuccess(row.id(), rowsLoaded, dl.sizeBytes(), dl.sha256());
      return new IngestResult(false, "Ingested " + dataset + " " + yearMonth);

    } catch (Exception e) {
      manifest.markFailed(row.id(), e.getMessage());
      throw e;
    }
  }
}