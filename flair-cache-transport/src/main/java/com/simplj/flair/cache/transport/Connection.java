package com.simplj.flair.cache.transport;

import java.net.InetAddress;
import java.util.UUID;

public interface Connection {
    UUID id();
    InetAddress remoteAddress();
    void send(RawFrame frame);
    void close();
    boolean isAlive();
}
