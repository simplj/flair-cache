package com.simplj.flair.cache.benchmarks;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Writes {@code results-summary.json} after every benchmark run.
 *
 * <p>Complements the raw JMH {@code results.json} with data that matters most when
 * comparing benchmark runs:</p>
 * <ul>
 *   <li><b>Run config</b>: threads, forks, warmup iterations + time, measurement iterations + time</li>
 *   <li><b>Latency</b>: mean, meanError, p50/p90/p99/p999 (from {@link Mode#SampleTime})</li>
 *   <li><b>TPS</b>: ops/sec derived from {@link Mode#Throughput}; falls back to
 *       {@code 1 / mean_latency} when throughput mode was not run</li>
 * </ul>
 *
 * <p>SampleTime and Throughput results for the same benchmark are merged into one entry.
 * Results for benchmarks with {@code @Param} annotations are kept separate (one entry per
 * param-value combination).</p>
 *
 * <p>Pure JDK — no external JSON library.</p>
 */
final class BenchmarkSummaryWriter {

    private BenchmarkSummaryWriter() {}

    static void write(Collection<RunResult> results, EnvironmentInfo env, String outputPath) throws IOException {
        // Group SampleTime + Throughput RunResult pairs by (benchmarkName + params)
        Map<String, SummaryEntry> grouped = new LinkedHashMap<>();
        for (RunResult rr : results) {
            String key = groupKey(rr.getParams());
            grouped.computeIfAbsent(key, k -> new SummaryEntry(rr.getParams())).add(rr);
        }

        StringBuilder sb = new StringBuilder(8192).append("{\n");
        env.appendJson(sb);
        sb.append(",\n  \"results\": [\n");
        boolean first = true;
        for (SummaryEntry entry : grouped.values()) {
            if (!first) sb.append(",\n");
            first = false;
            entry.appendJson(sb);
        }
        sb.append("\n  ]\n}\n");

        File file = new File(outputPath);
        file.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            pw.print(sb);
        }
        System.out.printf("[BenchmarkSummaryWriter] %d benchmark(s) summarised → %s%n",
                grouped.size(), file.getAbsolutePath());
    }

    // ── helpers ───────────────────────────────────────────────────────────────────

    /** Stable grouping key: fully-qualified method name + sorted @Param entries. */
    private static String groupKey(BenchmarkParams p) {
        StringBuilder sb = new StringBuilder(p.getBenchmark());
        Collection<String> paramKeys = p.getParamsKeys();
        if (paramKeys != null && !paramKeys.isEmpty()) {
            List<String> sorted = new ArrayList<>(paramKeys);
            Collections.sort(sorted);
            for (String k : sorted) {
                sb.append(':').append(k).append('=').append(p.getParam(k));
            }
        }
        return sb.toString();
    }

    /** Converts a Throughput score (ops/unit) to ops/sec. */
    private static double throughputToTps(double score, String unit) {
        if ("ops/ns".equals(unit))              return score * 1_000_000_000.0;
        if ("ops/us".equals(unit) || "ops/µs".equals(unit)) return score * 1_000_000.0;
        if ("ops/ms".equals(unit))              return score * 1_000.0;
        if ("ops/s".equals(unit))               return score;
        return Double.NaN;
    }

    /** Converts a SampleTime mean (unit/op) to ops/sec. */
    private static double sampleTimeToTps(double mean, String unit) {
        if ("ns/op".equals(unit))              return 1_000_000_000.0 / mean;
        if ("us/op".equals(unit) || "µs/op".equals(unit)) return 1_000_000.0 / mean;
        if ("ms/op".equals(unit))              return 1_000.0 / mean;
        if ("s/op".equals(unit))               return 1.0 / mean;
        return Double.NaN;
    }

    private static String q(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /** 3 decimal places; {@code null} for non-finite values. */
    private static String fmt(double v) {
        return Double.isFinite(v) ? String.format(Locale.ROOT, "%.3f", v) : "null";
    }

    /** Rounded integer for TPS; {@code null} for non-finite or negative. */
    private static String fmtTps(double tps) {
        return (Double.isFinite(tps) && tps >= 0)
                ? String.format(Locale.ROOT, "%.0f", tps)
                : "null";
    }

    @SuppressWarnings("rawtypes")
    private static String percentile(Map<String, Result> sec, String key) {
        if (sec == null) return "null";
        Result r = sec.get(key);
        return r == null ? "null" : fmt(r.getScore());
    }

    // ── inner class ───────────────────────────────────────────────────────────────

    private static final class SummaryEntry {
        private final BenchmarkParams params;
        private RunResult sampleTime;
        private RunResult throughput;

        SummaryEntry(BenchmarkParams params) { this.params = params; }

        void add(RunResult rr) {
            if (rr.getParams().getMode() == Mode.SampleTime) sampleTime = rr;
            else if (rr.getParams().getMode() == Mode.Throughput) throughput = rr;
        }

        void appendJson(StringBuilder sb) {
            sb.append("  {\n");

            // Benchmark name
            sb.append("    \"benchmark\": ").append(q(params.getBenchmark())).append(",\n");

            // @Param values (empty object if no @Param)
            sb.append("    \"params\": {");
            Collection<String> paramKeys = params.getParamsKeys();
            if (paramKeys != null && !paramKeys.isEmpty()) {
                boolean first = true;
                for (String k : paramKeys) {
                    if (!first) sb.append(", ");
                    first = false;
                    sb.append(q(k)).append(": ").append(q(params.getParam(k)));
                }
            }
            sb.append("},\n");

            // Run configuration
            IterationParams wu = params.getWarmup();
            IterationParams ms = params.getMeasurement();
            sb.append("    \"threads\": ").append(params.getThreads()).append(",\n");
            sb.append("    \"forks\": ").append(params.getForks()).append(",\n");
            sb.append("    \"warmup\": {")
              .append("\"iterations\": ").append(wu.getCount())
              .append(", \"time\": ").append(q(wu.getTime().toString()))
              .append("},\n");
            sb.append("    \"measurement\": {")
              .append("\"iterations\": ").append(ms.getCount())
              .append(", \"time\": ").append(q(ms.getTime().toString()))
              .append("},\n");

            // Latency from SampleTime
            if (sampleTime != null) {
                Result<?> pr = sampleTime.getPrimaryResult();
                @SuppressWarnings("rawtypes")
                Map<String, Result> sec = sampleTime.getSecondaryResults();
                sb.append("    \"latency\": {\n");
                sb.append("      \"unit\": ").append(q(pr.getScoreUnit())).append(",\n");
                sb.append("      \"mean\": ").append(fmt(pr.getScore())).append(",\n");
                sb.append("      \"meanError\": ").append(fmt(pr.getScoreError())).append(",\n");
                sb.append("      \"p50\": ").append(percentile(sec, "p0.50")).append(",\n");
                sb.append("      \"p90\": ").append(percentile(sec, "p0.90")).append(",\n");
                sb.append("      \"p99\": ").append(percentile(sec, "p0.99")).append(",\n");
                sb.append("      \"p999\": ").append(percentile(sec, "p0.999")).append("\n");
                sb.append("    },\n");
            } else {
                sb.append("    \"latency\": null,\n");
            }

            // TPS (ops/sec): prefer Throughput mode score; fall back to 1/mean from SampleTime
            double tps = Double.NaN;
            if (throughput != null) {
                Result<?> pr = throughput.getPrimaryResult();
                tps = throughputToTps(pr.getScore(), pr.getScoreUnit());
            } else if (sampleTime != null) {
                Result<?> pr = sampleTime.getPrimaryResult();
                tps = sampleTimeToTps(pr.getScore(), pr.getScoreUnit());
            }
            sb.append("    \"tps\": ").append(fmtTps(tps)).append("\n");

            sb.append("  }");
        }
    }
}
