package com.simplj.flair.cache.transport;

import javax.net.ssl.SSLContext;

public final class TlsConfig {

    private static final TlsConfig DISABLED = new TlsConfig(null, false, false);

    private final SSLContext sslContext;
    private final boolean enabled;
    private final boolean requireClientAuth;

    private TlsConfig(SSLContext sslContext, boolean enabled, boolean requireClientAuth) {
        this.sslContext = sslContext;
        this.enabled = enabled;
        this.requireClientAuth = requireClientAuth;
    }

    public static TlsConfig disabled() {
        return DISABLED;
    }

    public static TlsConfig of(SSLContext sslContext) {
        return new TlsConfig(sslContext, true, false);
    }

    public static TlsConfig withMutualAuth(SSLContext sslContext) {
        return new TlsConfig(sslContext, true, true);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    public boolean requireClientAuth() {
        return requireClientAuth;
    }
}
