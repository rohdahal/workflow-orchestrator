package io.github.rohandahal.orchestrator.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

@Service
public class TlcIngestManifestService {

  private final JdbcTemplate jdbc;

  public record ManifestRow(
      long id,
      String dataset,
      String yearMonth,
      String sourceFile,
      String status,
      long rowsLoaded,
      long fileSizeBytes,
      String checksum
  ) {}

  public TlcIngestManifestService(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Transactional
  public ManifestRow lockOrCreate(String dataset, String yearMonth, String sourceFile) {
    // Ensure row exists.
    jdbc.update("""
        insert into tlc_ingest_manifest(dataset, year_month, source_file, status, updated_at)
        values (?, ?, ?, 'in_progress', now())
        on conflict (dataset, year_month) do nothing
        """, dataset, yearMonth, sourceFile);

    // Lock row for decision making.
    return jdbc.queryForObject("""
        select id, dataset, year_month, source_file, status, rows_loaded, file_size_bytes, checksum
        from tlc_ingest_manifest
        where dataset = ? and year_month = ?
        for update
        """,
        (rs, rowNum) -> new ManifestRow(
            rs.getLong("id"),
            rs.getString("dataset"),
            rs.getString("year_month"),
            rs.getString("source_file"),
            rs.getString("status"),
            rs.getLong("rows_loaded"),
            rs.getLong("file_size_bytes"),
            rs.getString("checksum")
        ),
        dataset, yearMonth
    );
  }

  public void markSuccess(long id, long rowsLoaded, long fileSizeBytes, String checksum) {
    jdbc.update("""
        update tlc_ingest_manifest
        set status='success',
            rows_loaded=?,
            file_size_bytes=?,
            checksum=?,
            ingested_at=now(),
            error=null,
            updated_at=now()
        where id=?
        """, rowsLoaded, fileSizeBytes, checksum, id);
  }

  public void markFailed(long id, String error) {
    jdbc.update("""
        update tlc_ingest_manifest
        set status='failed',
            error=?,
            updated_at=now()
        where id=?
        """, error, id);
  }
}