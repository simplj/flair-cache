package com.simplj.flair.cache.bootstrap;

public final class SyncTimeoutException extends Exception {

    public SyncTimeoutException(String message) {
        super(message);
    }

    public SyncTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
