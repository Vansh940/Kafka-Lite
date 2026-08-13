package com.kafkalite.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * Single-threaded NIO event loop: accepts connections and dispatches full
 * frames to ConnectionHandler. Each SelectionKey gets a per-connection
 * read buffer attached so partial TCP reads across multiple OP_READ events
 * are accumulated correctly before a full frame is dispatched.
 */
public class NioServer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NioServer.class);

    private final int port;
    private final ConnectionHandler handler;
    private Selector selector;
    private volatile boolean running = true;

    public NioServer(int port, ConnectionHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    @Override
    public void run() {
        try {
            selector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(port));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            log.info("NioServer listening on port {}", port);

            while (running) {
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    try {
                        if (!key.isValid()) continue;
                        if (key.isAcceptable()) accept(key);
                        else if (key.isReadable()) read(key);
                    } catch (IOException e) {
                        log.warn("Connection error, closing channel", e);
                        key.channel().close();
                        key.cancel();
                    }
                }
            }
        } catch (IOException e) {
            log.error("NioServer fatal error", e);
        }
    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        client.configureBlocking(false);
        // Attach a FrameAccumulator so partial reads persist across events on this connection.
        client.register(selector, SelectionKey.OP_READ, new FrameAccumulator());
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        FrameAccumulator acc = (FrameAccumulator) key.attachment();

        ByteBuffer chunk = ByteBuffer.allocate(8192);
        int n = channel.read(chunk);
        if (n < 0) { channel.close(); key.cancel(); return; }
        chunk.flip();
        acc.feed(chunk);

        ByteBuffer frame;
        while ((frame = acc.nextFrame()) != null) {
            ByteBuffer response = handler.handle(frame);
            ByteBuffer lenPrefix = ByteBuffer.allocate(4).putInt(response.remaining());
            lenPrefix.flip();
            channel.write(lenPrefix);
            channel.write(response);
        }
    }

    public void stop() { running = false; if (selector != null) selector.wakeup(); }
}
