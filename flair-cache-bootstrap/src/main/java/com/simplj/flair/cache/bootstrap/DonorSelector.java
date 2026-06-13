package com.simplj.flair.cache.bootstrap;

import com.simplj.flair.cache.transport.Connection;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.TcpClient;
import com.simplj.flair.cache.transport.TlsConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Selects a bootstrap donor from a configured list of seed addresses. Tries seeds
 * round-robin with exponential backoff until one accepts a connection or the timeout
 * elapses.
 *
 * <p>Returns a {@link DonorConnection} that must be closed after sync completes.</p>
 */
public final class DonorSelector {

    private static final Logger log = Logger.getLogger(DonorSelector.class.getName());
    private static final long[] BACKOFFS_MS = {100, 200, 400, 800, 1600, 3200, 6400, 10_000};

    public record SeedAddress(String host, int port) {}

    /**
     * Holds the open donor connection and its owning TcpClient so both can be shut down
     * together after sync completes.
     */
    public static final class DonorConnection {
        private final Connection connection;
        private final TcpClient client;

        DonorConnection(Connection connection, TcpClient client) {
            this.connection = connection;
            this.client = client;
        }

        public Connection connection() { return connection; }

        public void close() {
            connection.close();
            client.shutdown();
        }
    }

    private final List<SeedAddress> seeds;
    private final int connectTimeoutMs;
    private final TlsConfig tls;

    private DonorSelector(Builder b) {
        this.seeds = List.copyOf(b.seeds);
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.tls = b.tls;
    }

    /**
     * Tries each seed address in round-robin order with exponential backoff until a
     * connection succeeds or {@code totalTimeoutMs} elapses.
     *
     * @throws SyncTimeoutException if all seeds remain unreachable within {@code totalTimeoutMs}
     * @throws IOException          if seeds are exhausted immediately (no timeout left)
     */
    public DonorConnection select(FrameHandler handler, long totalTimeoutMs)
            throws IOException, SyncTimeoutException {
        long deadline = System.currentTimeMillis() + totalTimeoutMs;
        int round = 0;
        IOException lastError = null;

        while (System.currentTimeMillis() < deadline) {
            for (SeedAddress seed : seeds) {
                if (System.currentTimeMillis() >= deadline) break;
                TcpClient client = TcpClient.builder()
                        .remoteAddress(seed.host())
                        .remotePort(seed.port())
                        .connectTimeoutMs(connectTimeoutMs)
                        .handler(handler)
                        .tls(tls)
                        .workerThreads(1) // single worker preserves frame order: SYNC_DONE fires only after all SYNC_CHUNKs are applied
                        .build();
                try {
                    Connection conn = client.connect();
                    if (log.isLoggable(Level.FINE)) {
                        log.fine("Bootstrap donor selected: " + seed.host() + ":" + seed.port());
                    }
                    return new DonorConnection(conn, client);
                } catch (IOException e) {
                    client.shutdown();
                    lastError = e;
                    log.warning("Donor " + seed.host() + ":" + seed.port()
                            + " unreachable: " + e.getMessage());
                }
            }
            long backoffMs = BACKOFFS_MS[Math.min(round++, BACKOFFS_MS.length - 1)];
            long remaining = deadline - System.currentTimeMillis();
            if (remaining > 0) {
                LockSupport.parkNanos(Math.min(backoffMs, remaining) * 1_000_000L);
            }
        }
        throw new SyncTimeoutException(
                "All donor seeds unreachable within " + totalTimeoutMs + "ms", lastError);
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private final List<SeedAddress> seeds = new ArrayList<>();
        private int connectTimeoutMs = 3_000;
        private TlsConfig tls = TlsConfig.disabled();

        public Builder seed(String host, int port) {
            seeds.add(new SeedAddress(host, port));
            return this;
        }

        public Builder connectTimeoutMs(int ms) {
            if (ms <= 0) throw new IllegalArgumentException("connectTimeoutMs must be positive");
            this.connectTimeoutMs = ms;
            return this;
        }

        public Builder tls(TlsConfig tls) {
            this.tls = Objects.requireNonNull(tls, "tls must not be null");
            return this;
        }

        public DonorSelector build() {
            if (seeds.isEmpty()) {
                throw new IllegalStateException("At least one seed address must be configured");
            }
            return new DonorSelector(this);
        }
    }
}
