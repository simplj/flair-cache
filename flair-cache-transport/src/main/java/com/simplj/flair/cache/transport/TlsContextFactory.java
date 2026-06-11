package com.simplj.flair.cache.transport;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;

public final class TlsContextFactory {

    private TlsContextFactory() {}

    public static SSLContext serverContext(
            InputStream keyStoreStream, char[] keyStorePassword,
            InputStream trustStoreStream, char[] trustStorePassword)
            throws GeneralSecurityException, IOException {
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(
                kmf(keyStoreStream, keyStorePassword).getKeyManagers(),
                tmf(trustStoreStream, trustStorePassword).getTrustManagers(),
                null);
        return ctx;
    }

    public static SSLContext clientContext(
            InputStream trustStoreStream, char[] trustStorePassword)
            throws GeneralSecurityException, IOException {
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(null, tmf(trustStoreStream, trustStorePassword).getTrustManagers(), null);
        return ctx;
    }

    public static SSLContext clientContextWithCert(
            InputStream keyStoreStream, char[] keyStorePassword,
            InputStream trustStoreStream, char[] trustStorePassword)
            throws GeneralSecurityException, IOException {
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(
                kmf(keyStoreStream, keyStorePassword).getKeyManagers(),
                tmf(trustStoreStream, trustStorePassword).getTrustManagers(),
                null);
        return ctx;
    }

    private static KeyManagerFactory kmf(InputStream ks, char[] password)
            throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(ks, password);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, password);
        return kmf;
    }

    private static TrustManagerFactory tmf(InputStream ts, char[] password)
            throws GeneralSecurityException, IOException {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(ts, password);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        return tmf;
    }
}
