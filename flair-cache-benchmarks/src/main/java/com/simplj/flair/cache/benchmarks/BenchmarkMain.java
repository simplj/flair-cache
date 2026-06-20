package com.simplj.flair.cache.benchmarks;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.BenchmarkList;
import org.openjdk.jmh.runner.BenchmarkListEntry;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.format.OutputFormat;
import org.openjdk.jmh.runner.format.OutputFormatFactory;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;

/**
 * Entry point for running all FlairCache benchmarks.
 *
 * <p>Discovers all {@code *Benchmark} classes on the classpath, runs them, and writes:</p>
 * <ul>
 *   <li>{@code benchmark-results/results.json} — raw JMH JSON (all per-iteration data)</li>
 *   <li>{@code benchmark-results/results-summary.json} — human-readable JSON with environment
 *       info, TPS, latency percentiles, and run configuration (threads, warmup, measurement)</li>
 * </ul>
 *
 * <p>Usage (full suite): {@code java -jar target/benchmarks.jar}</p>
 * <p>Filter by name: {@code java -jar target/benchmarks.jar LocalStoreBenchmark}</p>
 * <p>With JMH flags: {@code java -jar target/benchmarks.jar LocalStoreBenchmark -wi 1 -i 1 -f 1}</p>
 */
public final class BenchmarkMain {

    public static void main(String[] args) throws Exception {
        new File("benchmark-results").mkdirs();

        // Capture environment once, before any benchmark work starts.
        EnvironmentInfo env = EnvironmentInfo.capture();
        env.printSummary();

        ChainedOptionsBuilder builder = new OptionsBuilder()
                .resultFormat(ResultFormatType.JSON)
                .result("benchmark-results/results.json");

        boolean addDefaultIncludes;
        if (args.length == 0) {
            addDefaultIncludes = true;
        } else {
            try {
                CommandLineOptions cli = new CommandLineOptions(args);
                builder = builder.parent(cli);
                // If no benchmark pattern was given (only flags like -wi/-i/-f), add full suite
                addDefaultIncludes = cli.getIncludes().isEmpty();
            } catch (CommandLineOptionException e) {
                System.err.println("Invalid benchmark options: " + e.getMessage());
                System.exit(1);
                return;
            }
        }

        if (addDefaultIncludes) {
            builder = builder
                    .include("com\\.simplj\\.flair\\.cache\\.benchmarks\\..*Benchmark")
                    .include("com\\.simplj\\.flair\\.cache\\.gossip\\.GossipCodecBenchmark")
                    .include("com\\.simplj\\.flair\\.cache\\.replication\\.ReplicationFrameCodecBenchmark")
                    .include("com\\.simplj\\.flair\\.cache\\.bootstrap\\.BootstrapChunkCodecBenchmark");
        }

        Options options = builder.build();

        int totalBenchmarks = countBenchmarks(options);

        VerboseMode verbosity = options.verbosity().orElse(VerboseMode.NORMAL);
        OutputFormat jmhFormat = OutputFormatFactory.createFormatInstance(System.out, verbosity);
        OutputFormat trackingFormat = new ProgressTrackingOutputFormat(jmhFormat, totalBenchmarks);

        Collection<RunResult> results = new Runner(options, trackingFormat).run();
        BenchmarkSummaryWriter.write(results, env, "benchmark-results/results-summary.json");
    }

    /**
     * Counts the total number of benchmark runs (param combinations × forks) that JMH will
     * execute for the given options, so the progress display can show {@code [done/total]}.
     *
     * <p>Uses {@link BenchmarkList#find} with a silent output format to enumerate the same set
     * of entries that {@code Runner.run()} will iterate over.</p>
     */
    private static int countBenchmarks(Options options) {
        try (PrintStream devNull = new PrintStream(OutputStream.nullOutputStream())) {
            OutputFormat silent = OutputFormatFactory.createFormatInstance(devNull, VerboseMode.SILENT);
            SortedSet<BenchmarkListEntry> entries = BenchmarkList.defaultList()
                    .find(silent, options.getIncludes(), options.getExcludes());
            if (entries == null || entries.isEmpty()) {
                return 0;
            }
            int globalForks = options.getForkCount().orElse(5); // 5 is JMH's built-in default
            int total = 0;
            for (BenchmarkListEntry entry : entries) {
                int paramCombinations = 1;
                org.openjdk.jmh.util.Optional<Map<String, String[]>> paramsOpt = entry.getParams();
                if (paramsOpt.hasValue()) {
                    for (String[] values : paramsOpt.get().values()) {
                        paramCombinations *= values.length;
                    }
                }
                int forks = entry.getForks().hasValue() ? entry.getForks().get() : globalForks;
                total += paramCombinations * forks;
            }
            return total;
        } catch (Exception e) {
            // Non-fatal: fall back to unknown total; progress will show [done/?]
            return 0;
        }
    }
}
