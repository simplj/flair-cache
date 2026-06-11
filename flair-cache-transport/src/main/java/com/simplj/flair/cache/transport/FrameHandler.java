package com.simplj.flair.cache.transport;

@FunctionalInterface
public interface FrameHandler {
    void onFrame(Connection source, RawFrame frame);
}
