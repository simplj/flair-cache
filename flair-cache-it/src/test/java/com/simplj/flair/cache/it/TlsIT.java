package com.simplj.flair.cache.it;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.transport.TlsConfig;
import com.simplj.flair.cache.transport.TlsContextFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.simplj.flair.cache.it.ITSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenario 11 — TLS: 2-node cluster with mTLS enabled, replication succeeds;
 * mismatched certificate authority, replication does not succeed.
 *
 * <h2>Certificate setup</h2>
 * Two independent self-signed CA pairs are generated once per class via {@code keytool}:
 * <ul>
 *   <li><b>caA</b>: keystoreA / truststoreA — nodes using CA-A trust each other.</li>
 *   <li><b>caB</b>: keystoreB / truststoreB — nodes using CA-B trust each other but NOT CA-A.</li>
 * </ul>
 *
 * <p>The "wrong cert" test configures node-0 (server) with CA-A and node-1 (client) with CA-B.
 * The mTLS handshake must fail because node-1's cert is not signed by CA-A and vice-versa.</p>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class TlsIT {

    @TempDir
    static Path tempDir;

    private static TlsConfig tlsA;   // mTLS config backed by CA-A
    private static TlsConfig tlsB;   // mTLS config backed by CA-B (incompatible with CA-A)

    private FlairCache node0, node1;

    @BeforeAll
    static void generateCertificates() throws Exception {
        File ksA  = tempDir.resolve("ksA.jks").toFile();
        File tsA  = tempDir.resolve("tsA.jks").toFile();
        File ksB  = tempDir.resolve("ksB.jks").toFile();
        File tsB  = tempDir.resolve("tsB.jks").toFile();
        File cert = tempDir.resolve("cert.cer").toFile();

        // CA-A pair
        generateKeystore(ksA, "aliasA", "CN=FlairCA-A");
        exportCert(ksA, "aliasA", cert);
        importCert(tsA, cert, "aliasA");
        tlsA = TlsConfig.withMutualAuth(buildSslContext(ksA, tsA));

        // CA-B pair — independent self-signed cert, NOT trusted by tsA
        generateKeystore(ksB, "aliasB", "CN=FlairCA-B");
        exportCert(ksB, "aliasB", cert);
        importCert(tsB, cert, "aliasB");
        tlsB = TlsConfig.withMutualAuth(buildSslContext(ksB, tsB));
    }

    @AfterEach
    void tearDown() {
        if (node0 != null) { node0.shutdown(); node0 = null; }
        if (node1 != null) { node1.shutdown(); node1 = null; }
    }

    // ── Scenario 11a: mTLS succeeds with matching CAs ────────────────────────

    @Test
    void mtls_sameCA_2nodeCluster_replicationSucceeds() throws IOException {
        int p0 = freePort(), p1 = freePort();

        node0 = FlairCache.builder()
                .bindAddress("127.0.0.1").bindPort(p0)
                .tls(tlsA).consistency(ConsistencyMode.EVENTUAL)
                .build().start();

        node1 = FlairCache.builder()
                .bindAddress("127.0.0.1").bindPort(p1)
                .seedPeers(List.of("127.0.0.1:" + p0))
                .tls(tlsA).consistency(ConsistencyMode.EVENTUAL)
                .ackTimeoutMs(3000)
                .build().start();

        // Register blocks on both nodes BEFORE putting data, so node1 can receive frames.
        CacheBlock<String, String> b0 = node0.<String, String>registerBlock("tls-ok")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();
        CacheBlock<String, String> b1 = node1.<String, String>registerBlock("tls-ok")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();

        // Wait for SWIM gossip to form the cluster (both nodes see each other as ALIVE).
        // Once gossip has formed, the replication engine has initiated a TCP connection to the peer.
        // If the TLS handshake is still in progress when we write, the frame is held in the
        // per-peer pending buffer and flushed automatically once the handshake completes.
        boolean gossipFormed = awaitCondition(Duration.ofSeconds(10), 50,
                () -> node0.cluster().alive().size() >= 2 && node1.cluster().alive().size() >= 2);
        assertTrue(gossipFormed, "Nodes must discover each other via SWIM within 10 s");

        b0.put("secure-key", "secure-value");

        boolean replicated = awaitCondition(Duration.ofSeconds(5), 50,
                () -> "secure-value".equals(b1.get("secure-key")));
        assertTrue(replicated, "mTLS replication with matching CA must succeed within 5 s");
    }

    // ── Scenario 11b: mTLS fails with mismatched CAs ─────────────────────────

    @Test
    void mtls_differentCA_2nodeCluster_replicationDoesNotSucceed() throws IOException {
        int p0 = freePort(), p1 = freePort();

        // node0 trusts only CA-A; node1 presents a CA-B certificate.
        node0 = FlairCache.builder()
                .bindAddress("127.0.0.1").bindPort(p0)
                .tls(tlsA).consistency(ConsistencyMode.EVENTUAL)
                .build().start();

        node1 = FlairCache.builder()
                .bindAddress("127.0.0.1").bindPort(p1)
                .seedPeers(List.of("127.0.0.1:" + p0))
                .tls(tlsB).consistency(ConsistencyMode.EVENTUAL)
                .ackTimeoutMs(500)
                .build().start();

        CacheBlock<String, String> b0 = node0.<String, String>registerBlock("tls-fail")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();
        CacheBlock<String, String> b1 = node1.<String, String>registerBlock("tls-fail")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();

        // Wait for gossip to discover peers — gossip (UDP) is unaffected by TLS, so both nodes
        // will see each other as ALIVE. The replication TCP connection attempts will fail because
        // node1 presents a CA-B cert that the CA-A truststore does not recognise. Any frames
        // written to node0 for node1 will be held in the pending buffer but never flushed because
        // the TLS handshake never succeeds; data must not appear on node1.
        boolean gossipFormed = awaitCondition(Duration.ofSeconds(5), 50,
                () -> node0.cluster().alive().size() >= 2 && node1.cluster().alive().size() >= 2);
        assertTrue(gossipFormed, "Gossip must discover both nodes within 5 s (UDP is not TLS-gated)");

        b0.put("cross-ca-key", "should-not-replicate");

        // TLS handshake between CA-A server and CA-B client must fail.
        // We wait a further window and assert the data did NOT appear on node1.
        boolean replicated = awaitCondition(Duration.ofSeconds(3), 50,
                () -> b1.get("cross-ca-key") != null);
        assertFalse(replicated,
                "cross-CA mTLS: data must NOT replicate — TLS handshake must reject the mismatched cert");
        assertNull(b1.get("cross-ca-key"),
                "node1 (CA-B) must not receive data from node0 (CA-A) due to cert mismatch");
    }

    // ── Certificate helpers ───────────────────────────────────────────────────

    private static void generateKeystore(File ks, String alias, String dname) throws Exception {
        exec("keytool",
                "-genkeypair",
                "-alias", alias,
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "1",
                "-dname", dname,
                "-keystore", ks.getAbsolutePath(),
                "-storepass", "changeit",
                "-keypass", "changeit",
                "-storetype", "JKS");
    }

    private static void exportCert(File ks, String alias, File cert) throws Exception {
        exec("keytool",
                "-exportcert",
                "-alias", alias,
                "-keystore", ks.getAbsolutePath(),
                "-storepass", "changeit",
                "-file", cert.getAbsolutePath());
    }

    private static void importCert(File ts, File cert, String alias) throws Exception {
        exec("keytool",
                "-importcert",
                "-noprompt",
                "-alias", alias,
                "-file", cert.getAbsolutePath(),
                "-keystore", ts.getAbsolutePath(),
                "-storepass", "changeit",
                "-storetype", "JKS");
    }

    private static SSLContext buildSslContext(File ks, File ts) throws Exception {
        char[] pw = "changeit".toCharArray();
        return TlsContextFactory.serverContext(
                new FileInputStream(ks), pw,
                new FileInputStream(ts), pw);
    }

    private static void exec(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd)
                .redirectErrorStream(true)
                .start();
        int exit = p.waitFor();
        if (exit != 0) {
            byte[] out = p.getInputStream().readAllBytes();
            throw new RuntimeException("keytool failed (exit " + exit + "): " + new String(out));
        }
    }
}
