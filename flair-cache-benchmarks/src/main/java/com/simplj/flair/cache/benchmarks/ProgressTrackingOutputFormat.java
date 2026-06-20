package com.simplj.flair.cache.benchmarks;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.format.OutputFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps JMH's default {@link OutputFormat} to inject a {@code [done/total]} counter into
 * the {@code # Run progress:} line that JMH emits at the start of each benchmark.
 *
 * <p>JMH prints progress via {@code OutputFormat.println()} in {@code BaseRunner.etaBeforeBenchmark()},
 * which fires after {@code startBenchmark()} and before the benchmark actually runs. By the time
 * the progress line prints, {@code endBenchmark()} from the previous run has already been called,
 * so {@code completed} always reflects the number of benchmarks fully finished, consistent with
 * the percentage JMH shows.</p>
 */
final class ProgressTrackingOutputFormat implements OutputFormat {

    private static final String PROGRESS_MARKER = "# Run progress: ";

    private final OutputFormat delegate;
    private final int total;
    private final AtomicInteger completed = new AtomicInteger(0);

    ProgressTrackingOutputFormat(OutputFormat delegate, int total) {
        this.delegate = delegate;
        this.total = total;
    }

    /** Intercepts the {@code # Run progress:} line and injects {@code [done/total]}. */
    @Override
    public void println(String s) {
        if (s != null && s.contains(PROGRESS_MARKER)) {
            delegate.println(injectCounter(s));
        } else {
            delegate.println(s);
        }
    }

    /** Increments the completed counter so the next progress line reflects this completion. */
    @Override
    public void endBenchmark(BenchmarkResult result) {
        completed.incrementAndGet();
        delegate.endBenchmark(result);
    }

    private String injectCounter(String s) {
        int idx = s.indexOf(PROGRESS_MARKER);
        int insertAt = idx + PROGRESS_MARKER.length();
        int current = completed.get() + 1;
        String counter = total > 0
                ? "[" + current + "/" + total + "] "
                : "[" + current + "/?] ";
        return s.substring(0, insertAt) + counter + s.substring(insertAt);
    }

    // ── pure delegation ───────────────────────────────────────────────────────

    @Override
    public void iteration(BenchmarkParams params, IterationParams p, int i) {
        delegate.iteration(params, p, i);
    }

    @Override
    public void iterationResult(BenchmarkParams params, IterationParams p, int i, IterationResult r) {
        delegate.iterationResult(params, p, i, r);
    }

    @Override
    public void startBenchmark(BenchmarkParams params) {
        delegate.startBenchmark(params);
    }

    @Override
    public void startRun() {
        delegate.startRun();
    }

    @Override
    public void endRun(Collection<RunResult> results) {
        delegate.endRun(results);
    }

    @Override
    public void print(String s) {
        delegate.print(s);
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void verbosePrintln(String s) {
        delegate.verbosePrintln(s);
    }

    @Override
    public void write(int b) {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
    }
}
