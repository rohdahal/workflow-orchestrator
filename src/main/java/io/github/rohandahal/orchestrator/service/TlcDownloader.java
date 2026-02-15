package io.github.rohandahal.orchestrator.service;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
  private static final String ACCESS_KEY_ENV = "AWS_ACCESS_KEY_ID";
  private static final String SECRET_KEY_ENV = "AWS_SECRET_ACCESS_KEY";
  private static final String SESSION_TOKEN_ENV = "AWS_SESSION_TOKEN";

  @Value("${orchestrator.tlc.base-url}")
  private String baseUrl;

  private final HttpClient http;
  private final S3Client s3;
  private final Environment env;
  private final String bucket;
  private final String prefix;

  public record DownloadResult(String s3Uri, long sizeBytes, String sha256, boolean skipped) {}

  public TlcDownloader(Environment env) {
    this.env = env;
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(15))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();

    this.bucket = firstNonBlank(
        System.getenv(BUCKET_ENV),
        env.getProperty(BUCKET_ENV),
        env.getProperty("orchestrator.tlc.s3-bucket")
    );
    this.prefix = firstNonBlank(
        System.getenv(PREFIX_ENV),
        env.getProperty(PREFIX_ENV),
        env.getProperty("orchestrator.tlc.s3-prefix")
    );

    String region = firstNonBlank(
        System.getenv(REGION_ENV),
        env.getProperty(REGION_ENV),
        readDotEnv(REGION_ENV)
    );
    Region resolvedRegion = (region == null || region.isBlank())
        ? Region.US_EAST_1
        : Region.of(region);
    var builder = S3Client.builder().region(resolvedRegion);
    String accessKey = firstNonBlank(
        System.getenv(ACCESS_KEY_ENV),
        env.getProperty(ACCESS_KEY_ENV),
        readDotEnv(ACCESS_KEY_ENV)
    );
    String secretKey = firstNonBlank(
        System.getenv(SECRET_KEY_ENV),
        env.getProperty(SECRET_KEY_ENV),
        readDotEnv(SECRET_KEY_ENV)
    );
    String sessionToken = firstNonBlank(
        System.getenv(SESSION_TOKEN_ENV),
        env.getProperty(SESSION_TOKEN_ENV),
        readDotEnv(SESSION_TOKEN_ENV)
    );
    if (!accessKey.isBlank() && !secretKey.isBlank()) {
      if (!sessionToken.isBlank()) {
        builder.credentialsProvider(
            StaticCredentialsProvider.create(
                AwsSessionCredentials.create(accessKey, secretKey, sessionToken)
            )
        );
      } else {
        builder.credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
        );
      }
    }
    this.s3 = builder.build();
  }

  public DownloadResult downloadMonth(String dataset, String yearMonth, boolean force) {
    validate(dataset, yearMonth);
    String bucket = requireNonBlank(
        firstNonBlank(
            this.bucket,
            env.getProperty(BUCKET_ENV),
            env.getProperty("orchestrator.tlc.s3-bucket"),
            readDotEnv(BUCKET_ENV)
        ),
        BUCKET_ENV
    );
    String resolvedPrefix = firstNonBlank(
        this.prefix,
        env.getProperty(PREFIX_ENV),
        env.getProperty("orchestrator.tlc.s3-prefix"),
        readDotEnv(PREFIX_ENV)
    );

    String filename = dataset + "_tripdata_" + yearMonth + ".parquet";
    String key = buildKey(resolvedPrefix, dataset, filename);
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

    } catch (S3Exception s3e) {
      String details = s3e.awsErrorDetails() == null
          ? "unknown"
          : (s3e.awsErrorDetails().errorCode() + ": " + s3e.awsErrorDetails().errorMessage());
      String requestId = firstNonBlank(s3e.requestId());
      String extendedRequestId = firstNonBlank(s3e.extendedRequestId());
      String message = firstNonBlank(s3e.getMessage());
      throw new IllegalStateException(
          "TLC S3 upload failed (status=" + s3e.statusCode()
              + ", details=" + details
              + ", requestId=" + requestId
              + ", extendedRequestId=" + extendedRequestId
              + ", message=" + message + ")",
          s3e
      );
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

  private static String normalizePrefix(String p) {
    if (p == null) return "";
    String x = p.trim();
    while (x.startsWith("/")) x = x.substring(1);
    while (x.endsWith("/")) x = x.substring(0, x.length() - 1);
    return x;
  }

  private static String requireNonBlank(String value, String envName) {
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required env var: " + envName);
    }
    return value.trim();
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        return value.trim();
      }
    }
    return "";
  }

  private static String readDotEnv(String key) {
    try {
      Path envFile = Path.of(".env");
      if (!Files.exists(envFile)) {
        return "";
      }
      for (String line : Files.readAllLines(envFile)) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        int eq = trimmed.indexOf('=');
        if (eq <= 0) {
          continue;
        }
        String k = trimmed.substring(0, eq).trim();
        if (!key.equals(k)) {
          continue;
        }
        return trimmed.substring(eq + 1).trim();
      }
    } catch (Exception ignore) {
      // fall back to other sources
    }
    return "";
  }
}
