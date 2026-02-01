package io.github.rohandahal.orchestrator.service;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Set;

@Service
public class TlcDownloader {

  private static final Set<String> DATASETS = Set.of("yellow", "green", "fhv", "hvfhv");

  private static final String BUCKET_ENV = "ORCH_S3_BUCKET";
  private static final String PREFIX_ENV = "ORCH_S3_PREFIX";
  private static final String REGION_ENV = "AWS_REGION";

  @Value("${tlc.base-url}")
  private String baseUrl;

  private final HttpClient http;
  private final S3Client s3;
  private final String bucket;
  private final String prefix;

  public record DownloadResult(String s3Uri, long sizeBytes, String sha256, boolean skipped) {}

  public TlcDownloader() {
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(15))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();

    this.bucket = requireEnv(BUCKET_ENV);
    this.prefix = getenvOrEmpty(PREFIX_ENV);

    String region = System.getenv(REGION_ENV);
    if (region == null || region.isBlank()) {
      this.s3 = S3Client.create();
    } else {
      this.s3 = S3Client.builder().region(Region.of(region)).build();
    }
  }

  public DownloadResult downloadMonth(String dataset, String yearMonth, boolean force) {
    validate(dataset, yearMonth);

    String filename = dataset + "_tripdata_" + yearMonth + ".parquet";
    String key = buildKey(prefix, dataset, filename);
    String s3Uri = "s3://" + bucket + "/" + key;

    try {
      // Idempotent skip
      if (!force && s3ObjectExists(bucket, key)) {
        var head = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
        return new DownloadResult(s3Uri, head.contentLength(), "", true);
      }

      URI url = URI.create(baseUrl + "/" + filename);

      HttpRequest req = HttpRequest.newBuilder(url)
          .timeout(Duration.ofSeconds(60))
          .header("User-Agent", "workflow-orchestrator/1.0")
          .GET()
          .build();

      HttpResponse<InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
      if (resp.statusCode() != 200) {
        throw new IllegalStateException("Download failed HTTP " + resp.statusCode() + " for " + url);
      }

      String cl = resp.headers().firstValue("Content-Length").orElse(null);
      if (cl == null || cl.isBlank()) {
        throw new IllegalStateException("Missing Content-Length; required for streaming upload");
      }
      long contentLength = Long.parseLong(cl);

      // Stream CloudFront -> S3
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      try (InputStream raw = resp.body(); DigestInputStream in = new DigestInputStream(raw, md)) {
        PutObjectRequest putReq = PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .contentType("application/octet-stream")
            .build();

        s3.putObject(putReq, RequestBody.fromInputStream(in, contentLength));
      }

      String sha256 = HexFormat.of().formatHex(md.digest());
      return new DownloadResult(s3Uri, contentLength, sha256, false);

    } catch (Exception e) {
      throw new IllegalStateException("TLC S3 download failed: " + e.getMessage(), e);
    }
  }

  private boolean s3ObjectExists(String bucket, String key) {
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) return false;
      throw e;
    }
  }

  private static String buildKey(String prefix, String dataset, String filename) {
    String p = normalizePrefix(prefix);
    if (p.isEmpty()) return dataset + "/" + filename;
    return p + "/" + dataset + "/" + filename;
  }

  private void validate(String dataset, String yearMonth) {
    if (!DATASETS.contains(dataset)) throw new IllegalArgumentException("Invalid dataset: " + dataset);
    if (yearMonth == null || yearMonth.length() != 7 || yearMonth.charAt(4) != '-') {
      throw new IllegalArgumentException("year_month must be YYYY-MM");
    }
    int mm = Integer.parseInt(yearMonth.substring(5, 7));
    if (mm < 1 || mm > 12) throw new IllegalArgumentException("Invalid month: " + yearMonth);
  }

  private static String requireEnv(String name) {
    String v = System.getenv(name);
    if (v == null || v.isBlank()) throw new IllegalStateException("Missing required env var: " + name);
    return v.trim();
  }

  private static String getenvOrEmpty(String name) {
    String v = System.getenv(name);
    return v == null ? "" : v.trim();
  }

  private static String normalizePrefix(String p) {
    if (p == null) return "";
    String x = p.trim();
    while (x.startsWith("/")) x = x.substring(1);
    while (x.endsWith("/")) x = x.substring(0, x.length() - 1);
    return x;
  }
}