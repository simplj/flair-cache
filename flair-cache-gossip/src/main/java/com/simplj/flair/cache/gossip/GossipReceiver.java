package com.simplj.flair.cache.gossip;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

final class GossipReceiver implements Runnable {
    private static final Logger log = Logger.getLogger(GossipReceiver.class.getName());

    private final DatagramSocket socket;
    private final GossipNode     node;
    private volatile boolean     running;

    // Pre-allocated — reused every receive iteration (never allocated per packet)
    private final byte[]         recvBuffer = new byte[GossipProtocol.MAX_PACKET];
    private final DatagramPacket packet     = new DatagramPacket(recvBuffer, recvBuffer.length);

    GossipReceiver(DatagramSocket socket, GossipNode node) {
        this.socket = socket;
        this.node   = node;
    }

    void start() {
        running = true;
        new FlairCacheThreadFactory("flaircache-gossip-recv").newThread(this).start();
    }

    void stop() {
        running = false;
        socket.close();
    }

    @Override
    public void run() {
        while (running) {
            try {
                packet.setLength(recvBuffer.length);
                socket.receive(packet);
                GossipMessage msg = GossipProtocol.decode(recvBuffer, packet.getLength());
                InetAddress senderAddr = packet.getAddress();
                int         senderPort = packet.getPort();
                node.onMessage(msg, senderAddr, senderPort);
            } catch (IOException e) {
                if (running) log.log(Level.WARNING, "Gossip receive error", e);
            } catch (GossipProtocolException e) {
                log.log(Level.WARNING, "Malformed gossip packet", e);
            }
        }
    }
}
