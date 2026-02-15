package io.github.rohandahal.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


@Component
@ConfigurationProperties(prefix = "orchestrator")
public class OrchestratorProperties {

  private final Tlc tlc = new Tlc();

  public Tlc getTlc() {
    return tlc;
  }

  public static class Tlc {
    // CloudFront base URL for TLC downloads (configured via orchestrator.tlc.base-url)
    private String baseUrl;

    // Read from env via Spring relaxed binding:
    // ORCH_S3_BUCKET
    // ORCH_S3_PREFIX
    private String s3Bucket;
    private String s3Prefix;

    public String getBaseUrl() {
      if (baseUrl == null || baseUrl.isBlank()) {
        throw new IllegalStateException("orchestrator.tlc.base-url is not set");
      }
      return baseUrl;
    }
    public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }

    public String getS3Bucket() { return s3Bucket; }
    public void setS3Bucket(String s3Bucket) { this.s3Bucket = s3Bucket; }

    public String getS3Prefix() { return s3Prefix; }
    public void setS3Prefix(String s3Prefix) { this.s3Prefix = s3Prefix; }

    public String s3aPrefix() {
      if (s3Bucket == null || s3Bucket.isBlank()) {
        throw new IllegalStateException("orchestrator.tlc.s3Bucket is not set (env ORCH_S3_BUCKET)");
      }
      if (s3Prefix == null || s3Prefix.isBlank()) {
        return "s3a://" + s3Bucket;
      }
      return "s3a://" + s3Bucket + "/" + s3Prefix;
    }
  }
}