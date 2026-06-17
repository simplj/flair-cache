package com.simplj.flair.cache.benchmarks;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.Collection;

/**
 * Entry point for running all FlairCache benchmarks.
 *
 * <p>Discovers all {@code *Benchmark} classes on the classpath, runs them, and writes:</p>
 * <ul>
 *   <li>{@code benchmark-results/results.json} — raw JMH JSON (all per-iteration data)</li>
 *   <li>{@code benchmark-results/results-summary.json} — human-readable JSON with TPS,
 *       latency percentiles, and run configuration (threads, warmup, measurement)</li>
 * </ul>
 *
 * <p>Usage (full suite): {@code java -jar target/benchmarks.jar}</p>
 * <p>Filter by name: {@code java -jar target/benchmarks.jar LocalStoreBenchmark}</p>
 * <p>With JMH flags: {@code java -jar target/benchmarks.jar LocalStoreBenchmark -wi 1 -i 1 -f 1}</p>
 */
public final class BenchmarkMain {

    public static void main(String[] args) throws Exception {
        new File("benchmark-results").mkdirs();

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

        Collection<RunResult> results = new Runner(builder.build()).run();
        BenchmarkSummaryWriter.write(results, "benchmark-results/results-summary.json");
    }
}
