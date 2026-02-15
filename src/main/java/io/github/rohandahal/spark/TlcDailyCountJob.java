package io.github.rohandahal.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * Spark job that computes daily trip counts from NYC TLC Parquet data.
 *
 * Standalone:
 * - Runnable via spark-submit
 * - Reads one month of Parquet from S3
 * - Deletes existing month rows (idempotent) then appends aggregates into Postgres via JDBC
 */
public final class TlcDailyCountJob {

  public static void main(String[] args) {
    Map<String, String> params = parseArgs(args);

    String dataset = require(params, "dataset");
    String yearMonth = require(params, "year_month");
    String s3Path = require(params, "s3_path");
    String jdbcUrl = require(params, "jdbc_url");
    String jdbcUser = require(params, "jdbc_user");
    String jdbcPassword = require(params, "jdbc_password");

    // Compute date boundaries for the requested month.
    LocalDate start = LocalDate.parse(yearMonth + "-01");
    LocalDate end = start.plusMonths(1);

    SparkSession spark = SparkSession.builder()
        .appName("TLC Daily Count " + dataset + " " + yearMonth)
        .getOrCreate();

    try {
      // Build the S3 parquet path using the downloader layout: <prefix>/<dataset>/<dataset>_tripdata_<YYYY-MM>.parquet
      String filename = dataset + "_tripdata_" + yearMonth + ".parquet";
      String parquetPath = buildParquetPath(s3Path, dataset, filename);

      // Read the monthly parquet file from S3.
      Dataset<Row> trips = spark.read().parquet(parquetPath);

      // Pick the correct pickup timestamp column per dataset.
      String pickupTsCol = pickupTimestampColumn(dataset);

      // Derive pickup_date and aggregate daily trip counts.
      Dataset<Row> dailyCounts = trips
          .withColumn("pickup_date", to_date(col(pickupTsCol)))
          .filter(col("pickup_date").geq(lit(start.toString()))
              .and(col("pickup_date").lt(lit(end.toString()))))
          .groupBy(col("pickup_date"))
          .agg(count(lit(1)).cast(DataTypes.LongType).alias("trip_count"))
          .withColumn("dataset", lit(dataset))
          .select(col("dataset"), col("pickup_date"), col("trip_count"));

      // Ensure idempotency for this dataset+month by deleting existing rows in Postgres first.
      deleteExistingMonthRows(jdbcUrl, jdbcUser, jdbcPassword, dataset, start, end);

      // JDBC properties for Postgres.
      Properties jdbcProps = new Properties();
      jdbcProps.put("user", jdbcUser);
      jdbcProps.put("password", jdbcPassword);
      jdbcProps.put("driver", "org.postgresql.Driver");

      // Write aggregates into Postgres.
      dailyCounts.write()
          .mode(SaveMode.Append)
          .jdbc(jdbcUrl, "tlc_daily_counts", jdbcProps);

    } finally {
      spark.stop();
    }
  }

  // Resolve pickup timestamp column names across TLC datasets.
  private static String pickupTimestampColumn(String dataset) {
    return switch (dataset) {
      case "yellow" -> "tpep_pickup_datetime";
      case "green" -> "lpep_pickup_datetime";
      case "fhv" -> "pickup_datetime";
      case "hvfhv" -> "pickup_datetime";
      default -> throw new IllegalArgumentException("Unsupported dataset: " + dataset);
    };
  }

  // Build the parquet path; supports passing either a base prefix or a dataset-specific prefix.
  private static String buildParquetPath(String s3Path, String dataset, String filename) {
    String base = stripTrailingSlash(s3Path);

    // If caller already passes .../<dataset>, don't duplicate the dataset segment.
    if (base.endsWith("/" + dataset)) {
      return base + "/" + filename;
    }

    return base + "/" + dataset + "/" + filename;
  }

  private static String stripTrailingSlash(String s) {
    if (s == null) return "";
    String x = s.trim();
    while (x.endsWith("/")) {
      x = x.substring(0, x.length() - 1);
    }
    return x;
  }

  // Delete existing rows for this dataset and month window (keeps writes idempotent).
  private static void deleteExistingMonthRows(
      String jdbcUrl,
      String jdbcUser,
      String jdbcPassword,
      String dataset,
      LocalDate start,
      LocalDate end
  ) {
    String sql = "delete from tlc_daily_counts where dataset = ? and pickup_date >= ? and pickup_date < ?";

    try (Connection c = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
         PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, dataset);
      ps.setObject(2, start);
      ps.setObject(3, end);
      ps.executeUpdate();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to delete existing month rows", e);
    }
  }

  // Parse CLI args in the form: --key value
  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < args.length - 1; i += 2) {
      String key = args[i];
      if (!key.startsWith("--")) {
        throw new IllegalArgumentException("Expected --key value, got: " + key);
      }
      map.put(key.substring(2), args[i + 1]);
    }
    return map;
  }

  // Require a mandatory arg.
  private static String require(Map<String, String> params, String key) {
    String value = params.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing required argument: --" + key);
    }
    return value;
  }
}