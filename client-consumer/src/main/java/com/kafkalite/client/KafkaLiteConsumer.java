package com.kafkalite.client;

import com.kafkalite.core.model.Record;
import com.kafkalite.network.protocol.FetchRequest;
import com.kafkalite.network.protocol.ProtocolCodec;
import com.kafkalite.network.protocol.RequestType;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KafkaLiteConsumer implements AutoCloseable {
    private final Socket socket;
    private long nextOffset = 0;

    public KafkaLiteConsumer(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
    }

    public void seek(long offset) { this.nextOffset = offset; }

    public List<Record> poll(String topic, int partition, int maxRecords) throws IOException {
        FetchRequest req = new FetchRequest(topic, partition, nextOffset, maxRecords);
        ByteBuffer framed = ProtocolCodec.frame(RequestType.FETCH.code, req.encodeBody());
        socket.getOutputStream().write(framed.array(), 0, framed.limit());

        byte[] lenBuf = new byte[4];
        readFully(lenBuf);
        int respLen = ByteBuffer.wrap(lenBuf).getInt();
        byte[] respBuf = new byte[respLen];
        readFully(respBuf);

        ByteBuffer resp = ByteBuffer.wrap(respBuf);
        resp.get(); // status
        int count = resp.getInt();
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Record r = Record.deserialize(resp);
            records.add(r);
            nextOffset = r.offset + 1;
        }
        return records;
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
