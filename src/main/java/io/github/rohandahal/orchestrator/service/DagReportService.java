package io.github.rohandahal.orchestrator.service;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.awt.Color;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

@Service
public class DagReportService {

  private static final String REPORT_BUCKET_ENV = "ORCH_REPORTS_S3_BUCKET";
  private static final String REPORT_PREFIX_ENV = "ORCH_REPORTS_S3_PREFIX";
  private static final String DATA_BUCKET_ENV = "ORCH_S3_BUCKET";
  private static final String DATA_PREFIX_ENV = "ORCH_S3_PREFIX";
  private static final String REGION_ENV = "AWS_REGION";
  private static final String ACCESS_KEY_ENV = "AWS_ACCESS_KEY_ID";
  private static final String SECRET_KEY_ENV = "AWS_SECRET_ACCESS_KEY";
  private static final String SESSION_TOKEN_ENV = "AWS_SESSION_TOKEN";

  private final JdbcTemplate jdbc;
  private final DagRunService dagRunService;
  private final S3Client s3;
  private final Environment env;

  public DagReportService(JdbcTemplate jdbc, DagRunService dagRunService, Environment env) {
    this.jdbc = jdbc;
    this.dagRunService = dagRunService;
    this.env = env;

    String region = firstNonBlank(
        System.getenv(REGION_ENV),
        env.getProperty(REGION_ENV),
        readDotEnv(REGION_ENV)
    );
    Region resolvedRegion = (region == null || region.isBlank()) ? Region.US_EAST_1 : Region.of(region);
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

  public void uploadIfDagSucceeded(long dagRunId) {
    DagRunService.DagRunContext ctx = dagRunService.getDagRunContext(dagRunId);
    if (!"success".equals(ctx.status())) {
      return;
    }

    Integer alreadyUploaded = jdbc.queryForObject(
        "select count(*) from dag_report where dag_run_id = ? and status = 'success'",
        Integer.class,
        dagRunId
    );
    if (alreadyUploaded != null && alreadyUploaded > 0) {
      return;
    }

    String bucket = firstNonBlank(
        System.getenv(REPORT_BUCKET_ENV),
        env.getProperty(REPORT_BUCKET_ENV),
        System.getenv(DATA_BUCKET_ENV),
        env.getProperty(DATA_BUCKET_ENV),
        env.getProperty("orchestrator.tlc.s3-bucket"),
        readDotEnv(REPORT_BUCKET_ENV),
        readDotEnv(DATA_BUCKET_ENV)
    );
    if (isBlank(bucket)) {
      throw new IllegalStateException("Missing report bucket env var. Set ORCH_REPORTS_S3_BUCKET or ORCH_S3_BUCKET.");
    }

    String basePrefix = firstNonBlank(
        System.getenv(REPORT_PREFIX_ENV),
        env.getProperty(REPORT_PREFIX_ENV),
        System.getenv(DATA_PREFIX_ENV),
        env.getProperty(DATA_PREFIX_ENV),
        env.getProperty("orchestrator.tlc.s3-prefix"),
        readDotEnv(REPORT_PREFIX_ENV),
        readDotEnv(DATA_PREFIX_ENV)
    );
    String reportPrefix = normalizePrefix(basePrefix) + "/reports";
    if (reportPrefix.startsWith("/")) {
      reportPrefix = reportPrefix.substring(1);
    }

    String filename = "dag-report-" + ctx.dagId() + "-" + ctx.dataset() + "-" +
        ctx.startDate() + "-" + ctx.endDate() + "-" + ctx.dagRunDbId() + ".pdf";
    String key = normalizePrefix(reportPrefix).isBlank() ? filename : normalizePrefix(reportPrefix) + "/" + filename;

    claimReportRow(ctx, key);

    try {
      byte[] pdf = buildReportPdf(ctx);
      PutObjectRequest put = PutObjectRequest.builder()
          .bucket(bucket)
          .key(key)
          .contentType("application/pdf")
          .build();
      s3.putObject(put, RequestBody.fromBytes(pdf));

      jdbc.update("""
          update dag_report
          set status = 'success', error = null, uploaded_at = now(), updated_at = now()
          where dag_run_id = ?
          """, dagRunId);
    } catch (S3Exception s3e) {
      String details = s3e.awsErrorDetails() == null
          ? "unknown"
          : (s3e.awsErrorDetails().errorCode() + ": " + s3e.awsErrorDetails().errorMessage());
      String requestId = firstNonBlank(s3e.requestId());
      String extendedRequestId = firstNonBlank(s3e.extendedRequestId());
      String message = firstNonBlank(s3e.getMessage());
      jdbc.update("""
          update dag_report
          set status = 'failed', error = ?, updated_at = now()
          where dag_run_id = ?
          """, truncate("S3 upload failed (status=" + s3e.statusCode()
          + ", details=" + details
          + ", requestId=" + requestId
          + ", extendedRequestId=" + extendedRequestId
          + ", message=" + message + ")"), dagRunId);
      throw new IllegalStateException("Failed to upload DAG report", s3e);
    } catch (Exception e) {
      jdbc.update("""
          update dag_report
          set status = 'failed', error = ?, updated_at = now()
          where dag_run_id = ?
          """, truncate(e.getMessage()), dagRunId);
      throw new IllegalStateException("Failed to upload DAG report", e);
    }
  }

  private void claimReportRow(DagRunService.DagRunContext ctx, String key) {
    jdbc.update("""
        insert into dag_report(dag_run_id, dag_id, dataset, report_key, status, created_at, updated_at)
        values (?, ?, ?, ?, 'in_progress', now(), now())
        on conflict (dag_run_id)
        do update set report_key = excluded.report_key, status = 'in_progress', error = null, updated_at = now()
        """, ctx.dagRunDbId(), ctx.dagId(), ctx.dataset(), key);
  }

  private byte[] buildReportPdf(DagRunService.DagRunContext ctx) {
    Summary summary = loadSummary(ctx.dataset(), ctx.startDate(), ctx.endDate());
    List<DailyPoint> series = loadDailySeries(ctx.dataset(), ctx.startDate(), ctx.endDate());
    if ("all".equals(ctx.dagId())) {
      List<ReportSection> sections = List.of(
          new ReportSection(
              "daily_tip_insights",
              buildDashboardForDag("daily_tip_insights", ctx, summary, series),
              analysesForDag("daily_tip_insights")
          ),
          new ReportSection(
              "monthly_revenue_rollup",
              buildDashboardForDag("monthly_revenue_rollup", ctx, summary, series),
              analysesForDag("monthly_revenue_rollup")
          ),
          new ReportSection(
              "driver_activity_summary",
              buildDashboardForDag("driver_activity_summary", ctx, summary, series),
              analysesForDag("driver_activity_summary")
          )
      );
      return RichPdfRenderer.renderSections(ctx, sections);
    }
    DashboardData dashboard = buildDashboard(ctx, summary, series);
    return RichPdfRenderer.render(ctx, dashboard, analysesForDag(ctx.dagId()));
  }

  private DashboardData buildDashboard(DagRunService.DagRunContext ctx, Summary summary, List<DailyPoint> series) {
    return buildDashboardForDag(ctx.dagId(), ctx, summary, series);
  }

  private DashboardData buildDashboardForDag(String dagId, DagRunService.DagRunContext ctx, Summary summary, List<DailyPoint> series) {
    List<ChartPoint> tripSeries = series.stream()
        .map(d -> new ChartPoint(d.date().format(DateTimeFormatter.ofPattern("MM-dd")), d.trips()))
        .toList();
    SeriesStats stats = computeSeriesStats(series);

    LocalDate peakDay = series.stream()
        .max(Comparator.comparingLong(DailyPoint::trips))
        .map(DailyPoint::date)
        .orElse(ctx.startDate());

    return switch (dagId) {
      case "daily_tip_insights" -> {
        String weekendVsWeekday = stats.weekdayAvg() == 0
            ? "No weekday baseline available."
            : "Weekend volume is " + formatSignedPercent((stats.weekendAvg() / stats.weekdayAvg()) - 1) + " vs weekdays.";
        String trendSignal = "Demand trend is " + formatSignedPercent(stats.trendPct()) +
            " (last 7 days vs first 7 days).";
        String concentrationSignal = "Top 3 days contribute " + formatPercent(stats.top3Share()) + " of total trips.";
        List<Kpi> kpis = List.of(
            new Kpi("Total Trips", formatNumber(summary.totalTrips()), "Across selected date range"),
            new Kpi("Avg Trips / Day", formatNumber(summary.avgTripsPerDay()), "Operational baseline"),
            new Kpi("Peak Day", peakDay + " (" + formatNumber(summary.maxTripsInDay()) + ")", "Busiest pickup day")
        );
        List<String> insights = List.of(
            trendSignal,
            weekendVsWeekday,
            concentrationSignal
        );
        yield new DashboardData(
            "Daily Tip Insights",
            "Dataset: " + ctx.dataset() + "  |  Range: " + ctx.startDate() + " to " + ctx.endDate(),
            kpis,
            "Daily Trips",
            tripSeries,
            "Top Pickup Days",
            topDays(series, 7, 1.0),
            insights
        );
      }
      case "monthly_revenue_rollup" -> {
        double revenuePerTrip = 18.75;
        List<ChartPoint> revenueSeries = series.stream()
            .map(d -> new ChartPoint(d.date().format(DateTimeFormatter.ofPattern("MM-dd")), d.trips() * revenuePerTrip))
            .toList();
        double totalRevenue = summary.totalTrips() * revenuePerTrip;
        double avgRevenue = summary.coveredDays() == 0 ? 0 : totalRevenue / summary.coveredDays();
        double projectedMonthlyRevenue = avgRevenue * ctx.startDate().lengthOfMonth();
        double top3RevenueShare = stats.top3Share();
        List<String> insights = List.of(
            "Daily revenue trend is " + formatSignedPercent(stats.trendPct()) + " across the period.",
            "Top 3 revenue days account for " + formatPercent(top3RevenueShare) + " of estimated monthly revenue.",
            "Projected full-month run-rate is " + formatCurrency(projectedMonthlyRevenue) + " (at current daily pace)."
        );
        yield new DashboardData(
            "Monthly Revenue Rollup",
            "Dataset: " + ctx.dataset() + "  |  Range: " + ctx.startDate() + " to " + ctx.endDate(),
            List.of(
                new Kpi("Est. Revenue", formatCurrency(totalRevenue), "Range total"),
                new Kpi("Avg Revenue / Day", formatCurrency(avgRevenue), "Daily run-rate"),
                new Kpi("Peak Daily Revenue", formatCurrency(summary.maxTripsInDay() * revenuePerTrip), "Best single day")
            ),
            "Estimated Daily Revenue",
            revenueSeries,
            "Top Revenue Days",
            topDays(series, 7, revenuePerTrip),
            insights
        );
      }
      case "driver_activity_summary" -> {
        double tripsPerDriver = 160.0;
        List<ChartPoint> driverSeries = series.stream()
            .map(d -> new ChartPoint(d.date().format(DateTimeFormatter.ofPattern("MM-dd")), Math.ceil(d.trips() / tripsPerDriver)))
            .toList();
        double avgDrivers = driverSeries.stream().mapToDouble(ChartPoint::value).average().orElse(0);
        double maxDrivers = driverSeries.stream().mapToDouble(ChartPoint::value).max().orElse(0);
        double pressure = avgDrivers == 0 ? 0 : (maxDrivers / avgDrivers);
        double activeDriversVolatility = driverSeries.stream()
            .mapToDouble(ChartPoint::value)
            .map(v -> Math.pow(v - avgDrivers, 2))
            .average()
            .orElse(0);
        activeDriversVolatility = avgDrivers == 0 ? 0 : Math.sqrt(activeDriversVolatility) / avgDrivers;
        List<String> insights = List.of(
            "Driver demand trend moved " + formatSignedPercent(stats.trendPct()) + " over the window.",
            "Peak-load pressure reached " + String.format(Locale.US, "%.2fx", pressure) + " relative to average demand.",
            "Estimated active-driver volatility is " + formatPercent(activeDriversVolatility) + " (coefficient of variation)."
        );
        yield new DashboardData(
            "Driver Activity Summary",
            "Dataset: " + ctx.dataset() + "  |  Range: " + ctx.startDate() + " to " + ctx.endDate(),
            List.of(
                new Kpi("Avg Active Drivers", formatNumber(Math.round(avgDrivers)), "Estimated per day"),
                new Kpi("Peak Active Drivers", formatNumber(Math.round(maxDrivers)), "Highest day"),
                new Kpi("Demand Pressure", String.format(Locale.US, "%.2fx", pressure), "Peak / average")
            ),
            "Estimated Active Drivers / Day",
            driverSeries,
            "Top Demand Days",
            topDays(series, 7, 1.0 / tripsPerDriver),
            insights
        );
      }
      case "all" -> {
        double revenuePerTrip = 18.75;
        double tripsPerDriver = 160.0;
        double estRevenue = summary.totalTrips() * revenuePerTrip;
        double avgDrivers = series.stream().mapToDouble(d -> Math.ceil(d.trips() / tripsPerDriver)).average().orElse(0);
        List<String> insights = List.of(
            "Composite demand trend: " + formatSignedPercent(stats.trendPct()) + " across the selected window.",
            "Operational concentration: top 3 days contributed " + formatPercent(stats.top3Share()) + " of trips.",
            "Estimated revenue and capacity indicate " + formatCurrency(estRevenue) + " at avg " + formatNumber(Math.round(avgDrivers)) + " active drivers/day."
        );
        yield new DashboardData(
            "Composite Operations Dashboard",
            "Dataset: " + ctx.dataset() + "  |  Range: " + ctx.startDate() + " to " + ctx.endDate(),
            List.of(
                new Kpi("Total Trips", formatNumber(summary.totalTrips()), "Volume baseline"),
                new Kpi("Est. Revenue", formatCurrency(estRevenue), "Financial signal"),
                new Kpi("Avg Active Drivers", formatNumber(Math.round(avgDrivers)), "Capacity signal")
            ),
            "Daily Trips",
            tripSeries,
            "Top Revenue Days",
            topDays(series, 7, revenuePerTrip),
            insights
        );
      }
      default -> new DashboardData(
          "Workflow Report",
          "Dataset: " + ctx.dataset() + "  |  Range: " + ctx.startDate() + " to " + ctx.endDate(),
          List.of(
              new Kpi("Total Trips", formatNumber(summary.totalTrips()), "Range total"),
              new Kpi("Avg Trips / Day", formatNumber(summary.avgTripsPerDay()), "Daily baseline"),
              new Kpi("Covered Days", formatNumber(summary.coveredDays()), "Data coverage")
          ),
          "Daily Trips",
          tripSeries,
          "Top Days",
          topDays(series, 7, 1.0),
          List.of("Custom workflow report")
      );
    };
  }

  private List<ChartPoint> topDays(List<DailyPoint> series, int n, double multiplier) {
    return series.stream()
        .sorted(Comparator.comparingLong(DailyPoint::trips).reversed())
        .limit(n)
        .sorted(Comparator.comparing(DailyPoint::date))
        .map(d -> new ChartPoint(d.date().format(DateTimeFormatter.ofPattern("MM-dd")), d.trips() * multiplier))
        .toList();
  }

  private SeriesStats computeSeriesStats(List<DailyPoint> series) {
    if (series.isEmpty()) {
      return new SeriesStats(0, 0, 0, 0, 0, 0);
    }
    double mean = series.stream().mapToLong(DailyPoint::trips).average().orElse(0);
    double variance = series.stream()
        .mapToDouble(d -> Math.pow(d.trips() - mean, 2))
        .average()
        .orElse(0);
    double stdDev = Math.sqrt(variance);
    double volatility = mean == 0 ? 0 : stdDev / mean;

    double weekendAvg = series.stream()
        .filter(d -> d.date().getDayOfWeek().getValue() >= 6)
        .mapToLong(DailyPoint::trips)
        .average()
        .orElse(0);
    double weekdayAvg = series.stream()
        .filter(d -> d.date().getDayOfWeek().getValue() <= 5)
        .mapToLong(DailyPoint::trips)
        .average()
        .orElse(0);

    int sample = Math.min(7, series.size());
    double firstWindow = series.subList(0, sample).stream().mapToLong(DailyPoint::trips).average().orElse(0);
    double lastWindow = series.subList(series.size() - sample, series.size()).stream().mapToLong(DailyPoint::trips).average().orElse(0);
    double trendPct = firstWindow == 0 ? 0 : ((lastWindow - firstWindow) / firstWindow);

    long total = series.stream().mapToLong(DailyPoint::trips).sum();
    long top3 = series.stream()
        .mapToLong(DailyPoint::trips)
        .boxed()
        .sorted(Comparator.reverseOrder())
        .limit(3)
        .mapToLong(Long::longValue)
        .sum();
    double top3Share = total == 0 ? 0 : ((double) top3 / total);

    return new SeriesStats(trendPct, top3Share, volatility, weekendAvg, weekdayAvg, mean);
  }

  private List<DailyPoint> loadDailySeries(String dataset, LocalDate startDate, LocalDate endDate) {
    List<DailyPoint> rows = jdbc.query("""
        select pickup_date, trip_count
        from tlc_daily_counts
        where dataset = ?
          and pickup_date >= ?
          and pickup_date <= ?
        order by pickup_date
        """, (rs, rowNum) -> new DailyPoint(
        rs.getDate("pickup_date").toLocalDate(),
        rs.getLong("trip_count")
    ), dataset, startDate, endDate);

    if (rows.isEmpty()) {
      List<DailyPoint> zero = new ArrayList<>();
      LocalDate cur = startDate;
      while (!cur.isAfter(endDate)) {
        zero.add(new DailyPoint(cur, 0));
        cur = cur.plusDays(1);
      }
      return zero;
    }

    List<DailyPoint> filled = new ArrayList<>();
    int idx = 0;
    LocalDate cur = startDate;
    while (!cur.isAfter(endDate)) {
      if (idx < rows.size() && rows.get(idx).date().equals(cur)) {
        filled.add(rows.get(idx));
        idx++;
      } else {
        filled.add(new DailyPoint(cur, 0));
      }
      cur = cur.plusDays(1);
    }
    return filled;
  }

  private List<String> analysesForDag(String dagId) {
    return switch (dagId) {
      case "daily_tip_insights" -> List.of(
          "Daily trip-volume trend line with first-vs-last 7-day direction signal.",
          "Top pickup-day concentration analysis (top-3 share and top-day ranking chart).",
          "Weekday vs weekend demand comparison to guide staffing windows."
      );
      case "monthly_revenue_rollup" -> List.of(
          "Estimated daily and total revenue from trip counts (fare assumption-based).",
          "Top revenue-day ranking with concentration share of the top 3 days.",
          "Revenue run-rate projection using observed average daily revenue."
      );
      case "driver_activity_summary" -> List.of(
          "Estimated active-driver demand per day from trip load assumptions.",
          "Demand-pressure metric (peak active drivers vs average active drivers).",
          "Driver-load volatility signal for utilization risk monitoring."
      );
      case "all" -> List.of(
          "Section 1: Daily demand behavior and concentration diagnostics.",
          "Section 2: Revenue estimates, top-day concentration, and run-rate.",
          "Section 3: Active-driver demand, pressure, and volatility indicators."
      );
      default -> List.of("Custom report analyses");
    };
  }

  public record Summary(long totalTrips, long coveredDays, long avgTripsPerDay, long maxTripsInDay) {}
  private record DailyPoint(LocalDate date, long trips) {}
  private record Kpi(String label, String value, String note) {}
  private record ChartPoint(String label, double value) {}
  private record SeriesStats(
      double trendPct,
      double top3Share,
      double volatility,
      double weekendAvg,
      double weekdayAvg,
      double mean
  ) {}
  private record DashboardData(
      String title,
      String subtitle,
      List<Kpi> kpis,
      String trendLabel,
      List<ChartPoint> trendSeries,
      String barLabel,
      List<ChartPoint> barSeries,
      List<String> insights
  ) {}
  private record ReportSection(String dagId, DashboardData dashboard, List<String> options) {}

  private Summary loadSummary(String dataset, LocalDate startDate, LocalDate endDate) {
    return jdbc.queryForObject("""
        select
          coalesce(sum(trip_count), 0) as total_trips,
          coalesce(count(*), 0) as covered_days,
          coalesce(avg(trip_count), 0) as avg_trips,
          coalesce(max(trip_count), 0) as max_trips
        from tlc_daily_counts
        where dataset = ?
          and pickup_date >= ?
          and pickup_date <= ?
        """, (rs, rowNum) -> new Summary(
        rs.getLong("total_trips"),
        rs.getLong("covered_days"),
        rs.getLong("avg_trips"),
        rs.getLong("max_trips")
    ), dataset, startDate, endDate);
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      if (!isBlank(value)) {
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

  private static String normalizePrefix(String p) {
    if (p == null) return "";
    String out = p.trim();
    while (out.startsWith("/")) out = out.substring(1);
    while (out.endsWith("/")) out = out.substring(0, out.length() - 1);
    return out;
  }

  private static String truncate(String msg) {
    if (msg == null) {
      return "report upload failed";
    }
    return msg.length() > 1200 ? msg.substring(0, 1200) : msg;
  }

  private static String formatNumber(long value) {
    return NumberFormat.getNumberInstance(Locale.US).format(value);
  }

  private static String formatCurrency(double value) {
    NumberFormat f = NumberFormat.getCurrencyInstance(Locale.US);
    f.setMaximumFractionDigits(0);
    return f.format(value);
  }

  private static String formatPercent(double value) {
    return String.format(Locale.US, "%.1f%%", value * 100);
  }

  private static String formatSignedPercent(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      return "0.0%";
    }
    return (value >= 0 ? "+" : "") + String.format(Locale.US, "%.1f%%", value * 100);
  }

  private static final class RichPdfRenderer {

    private static final PDFont FONT_REG = new PDType1Font(Standard14Fonts.FontName.HELVETICA);
    private static final PDFont FONT_BOLD = new PDType1Font(Standard14Fonts.FontName.HELVETICA_BOLD);

    private RichPdfRenderer() {
    }

    static byte[] render(DagRunService.DagRunContext ctx, DashboardData data, List<String> options) {
      return renderSections(ctx, List.of(new ReportSection(ctx.dagId(), data, options)));
    }

    static byte[] renderSections(DagRunService.DagRunContext ctx, List<ReportSection> sections) {
      try (PDDocument doc = new PDDocument()) {
        for (ReportSection section : sections) {
          PDPage page = new PDPage(PDRectangle.LETTER);
          doc.addPage(page);

          try (PDPageContentStream cs = new PDPageContentStream(doc, page)) {
            float margin = 40f;
            float width = page.getMediaBox().getWidth() - (2 * margin);
            float y = page.getMediaBox().getHeight() - margin;

            DashboardData data = section.dashboard();
            y = drawHeader(cs, margin, y, width, data.title(), data.subtitle(), section.dagId());
            y = drawKpis(cs, margin, y - 16, width, data.kpis());
            y = drawLineChart(cs, margin, y - 20, width, 180, data.trendLabel(), data.trendSeries());
            y = drawBarChart(cs, margin, y - 22, width, 140, data.barLabel(), data.barSeries());
            drawInsights(cs, margin, y - 18, width, data.insights(), section.options());
          }
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        doc.save(out);
        return out.toByteArray();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to render PDF report", e);
      }
    }

    private static float drawHeader(
        PDPageContentStream cs,
        float x,
        float yTop,
        float width,
        String title,
        String subtitle,
        String dagIdLabel
    ) throws IOException {
      float h = 82f;
      cs.setNonStrokingColor(new Color(20, 33, 61));
      cs.addRect(x, yTop - h, width, h);
      cs.fill();

      write(cs, FONT_BOLD, 22, Color.WHITE, x + 18, yTop - 30, title);
      write(cs, FONT_REG, 11, new Color(220, 230, 245), x + 18, yTop - 48, subtitle);
      write(cs, FONT_REG, 10, new Color(210, 220, 240), x + 18, yTop - 63,
          "Generated: " + OffsetDateTime.now() + "   |   DAG: " + dagIdLabel);
      return yTop - h;
    }

    private static float drawKpis(PDPageContentStream cs, float x, float yTop, float width, List<Kpi> kpis) throws IOException {
      float cardGap = 10f;
      float cardW = (width - (2 * cardGap)) / 3f;
      float h = 70f;
      for (int i = 0; i < 3 && i < kpis.size(); i++) {
        float cx = x + (i * (cardW + cardGap));
        Kpi k = kpis.get(i);

        cs.setNonStrokingColor(new Color(243, 246, 252));
        cs.addRect(cx, yTop - h, cardW, h);
        cs.fill();

        cs.setStrokingColor(new Color(220, 226, 238));
        cs.addRect(cx, yTop - h, cardW, h);
        cs.stroke();

        write(cs, FONT_REG, 10, new Color(70, 80, 98), cx + 10, yTop - 18, k.label());
        write(cs, FONT_BOLD, 15, new Color(12, 25, 48), cx + 10, yTop - 38, k.value());
        write(cs, FONT_REG, 9, new Color(95, 105, 120), cx + 10, yTop - 54, k.note());
      }
      return yTop - h;
    }

    private static float drawLineChart(
        PDPageContentStream cs,
        float x,
        float yTop,
        float width,
        float h,
        String label,
        List<ChartPoint> points
    ) throws IOException {
      drawSectionTitle(cs, x, yTop, label);
      float chartYTop = yTop - 16;
      float chartH = h - 22;
      float chartW = width;

      cs.setNonStrokingColor(new Color(250, 252, 255));
      cs.addRect(x, chartYTop - chartH, chartW, chartH);
      cs.fill();

      cs.setStrokingColor(new Color(220, 228, 242));
      cs.addRect(x, chartYTop - chartH, chartW, chartH);
      cs.stroke();

      if (points.isEmpty()) {
        write(cs, FONT_REG, 10, new Color(110, 120, 140), x + 12, chartYTop - 24, "No series data available");
        return chartYTop - chartH;
      }

      double max = points.stream().mapToDouble(ChartPoint::value).max().orElse(1d);
      max = Math.max(max, 1d);

      float pad = 16f;
      float innerX = x + pad;
      float innerY = chartYTop - chartH + pad;
      float innerW = chartW - (2 * pad);
      float innerH = chartH - (2 * pad);

      cs.setStrokingColor(new Color(188, 203, 228));
      cs.moveTo(innerX, innerY);
      cs.lineTo(innerX, innerY + innerH);
      cs.lineTo(innerX + innerW, innerY + innerH);
      cs.stroke();

      cs.setStrokingColor(new Color(58, 104, 201));
      float step = points.size() <= 1 ? innerW : innerW / (points.size() - 1);
      for (int i = 0; i < points.size(); i++) {
        float px = innerX + (i * step);
        float py = (float) (innerY + ((points.get(i).value() / max) * innerH));
        if (i == 0) {
          cs.moveTo(px, py);
        } else {
          cs.lineTo(px, py);
        }
      }
      cs.stroke();

      ChartPoint first = points.get(0);
      ChartPoint last = points.get(points.size() - 1);
      write(cs, FONT_REG, 9, new Color(90, 100, 120), innerX, innerY - 11, first.label());
      write(cs, FONT_REG, 9, new Color(90, 100, 120), innerX + innerW - 24, innerY - 11, last.label());
      write(cs, FONT_REG, 9, new Color(90, 100, 120), innerX + 2, innerY + innerH + 3, formatNumber(Math.round(max)));
      return chartYTop - chartH;
    }

    private static float drawBarChart(
        PDPageContentStream cs,
        float x,
        float yTop,
        float width,
        float h,
        String label,
        List<ChartPoint> bars
    ) throws IOException {
      drawSectionTitle(cs, x, yTop, label);
      float chartYTop = yTop - 16;
      float chartH = h - 22;

      cs.setNonStrokingColor(new Color(250, 252, 255));
      cs.addRect(x, chartYTop - chartH, width, chartH);
      cs.fill();

      cs.setStrokingColor(new Color(220, 228, 242));
      cs.addRect(x, chartYTop - chartH, width, chartH);
      cs.stroke();

      if (bars.isEmpty()) {
        write(cs, FONT_REG, 10, new Color(110, 120, 140), x + 12, chartYTop - 24, "No bar data available");
        return chartYTop - chartH;
      }

      double max = bars.stream().mapToDouble(ChartPoint::value).max().orElse(1d);
      max = Math.max(max, 1d);

      float pad = 16f;
      float innerX = x + pad;
      float innerY = chartYTop - chartH + pad;
      float innerW = width - (2 * pad);
      float innerH = chartH - (2 * pad);

      int n = bars.size();
      float slot = innerW / Math.max(1, n);
      float bw = Math.max(8f, slot - 8f);

      for (int i = 0; i < n; i++) {
        ChartPoint b = bars.get(i);
        float bh = (float) ((b.value() / max) * innerH);
        float bx = innerX + (i * slot) + ((slot - bw) / 2f);

        cs.setNonStrokingColor(new Color(92, 147, 255));
        cs.addRect(bx, innerY, bw, bh);
        cs.fill();

        write(cs, FONT_REG, 8, new Color(92, 102, 121), bx, innerY - 11, b.label());
      }

      write(cs, FONT_REG, 9, new Color(90, 100, 120), innerX + 2, innerY + innerH + 3, formatNumber(Math.round(max)));
      return chartYTop - chartH;
    }

    private static void drawInsights(
        PDPageContentStream cs,
        float x,
        float yTop,
        float width,
        List<String> insights,
        List<String> options
    ) throws IOException {
      drawSectionTitle(cs, x, yTop, "Insights and Options");
      float y = yTop - 18;

      for (String insight : insights) {
        y = writeWrappedBullet(cs, x + 4, y, width - 8, insight, new Color(48, 58, 78));
      }

      y -= 4;
      write(cs, FONT_BOLD, 10, new Color(35, 48, 73), x + 4, y, "Included analyses in this report:");
      y -= 12;
      for (String option : options) {
        y = writeWrappedBullet(cs, x + 4, y, width - 8, option, new Color(74, 88, 114));
      }
    }

    private static float writeWrappedBullet(
        PDPageContentStream cs,
        float x,
        float y,
        float width,
        String text,
        Color color
    ) throws IOException {
      String bullet = "- ";
      List<String> lines = wrap(FONT_REG, 9, text, width - 14);
      if (lines.isEmpty()) {
        return y;
      }
      write(cs, FONT_REG, 9, color, x, y, bullet + lines.get(0));
      float out = y - 11;
      for (int i = 1; i < lines.size(); i++) {
        write(cs, FONT_REG, 9, color, x + 11, out, lines.get(i));
        out -= 11;
      }
      return out;
    }

    private static void drawSectionTitle(PDPageContentStream cs, float x, float y, String title) throws IOException {
      write(cs, FONT_BOLD, 11, new Color(22, 38, 67), x, y, title);
    }

    private static void write(PDPageContentStream cs, PDFont font, float size, Color color, float x, float y, String text)
        throws IOException {
      cs.beginText();
      cs.setFont(font, size);
      cs.setNonStrokingColor(color);
      cs.newLineAtOffset(x, y);
      cs.showText(text == null ? "" : text);
      cs.endText();
    }

    private static List<String> wrap(PDFont font, float size, String text, float width) throws IOException {
      List<String> lines = new ArrayList<>();
      if (text == null || text.isBlank()) {
        return lines;
      }
      String[] words = text.split("\\s+");
      StringBuilder line = new StringBuilder();
      for (String word : words) {
        String candidate = line.length() == 0 ? word : line + " " + word;
        float w = (font.getStringWidth(candidate) / 1000f) * size;
        if (w <= width) {
          line = new StringBuilder(candidate);
        } else {
          if (line.length() > 0) {
            lines.add(line.toString());
          }
          line = new StringBuilder(word);
        }
      }
      if (line.length() > 0) {
        lines.add(line.toString());
      }
      return lines;
    }
  }
}
