package com.kafkalite.client;

import com.kafkalite.network.protocol.ProduceRequest;
import com.kafkalite.network.protocol.ProtocolCodec;
import com.kafkalite.network.protocol.RequestType;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

public class KafkaLiteProducer implements AutoCloseable {
    private final Socket socket;

    public KafkaLiteProducer(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
    }

    /** Returns the assigned offset (or -1 for acks=0, which doesn't wait for a response). */
    public synchronized long send(String topic, int partition, byte[] key, byte[] value, short acks) throws IOException {
        ProduceRequest req = new ProduceRequest(topic, partition, key, value, acks);
        ByteBuffer framed = ProtocolCodec.frame(RequestType.PRODUCE.code, req.encodeBody());
        socket.getOutputStream().write(framed.array(), 0, framed.limit());
        if (acks == 0) return -1;

        byte[] lenBuf = new byte[4];
        readFully(lenBuf);
        int respLen = ByteBuffer.wrap(lenBuf).getInt();
        byte[] respBuf = new byte[respLen];
        readFully(respBuf);

        ByteBuffer resp = ByteBuffer.wrap(respBuf);
        byte status = resp.get();
        if (status != 0) throw new IOException("Produce failed: " + ProtocolCodec.readString(resp));
        return resp.getLong();
    }

    private void readFully(byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int n = socket.getInputStream().read(buf, total, buf.length - total);
            if (n < 0) throw new IOException("Connection closed mid-response");
            total += n;
        }
    }

    @Override
    public void close() throws IOException { socket.close(); }
}
