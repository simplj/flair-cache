package com.simplj.flair.cache.replication;

public final class ReplicationTimeoutException extends Exception {

    private static final long serialVersionUID = 1L;

    public ReplicationTimeoutException(String message) {
        super(message);
    }

    public ReplicationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
