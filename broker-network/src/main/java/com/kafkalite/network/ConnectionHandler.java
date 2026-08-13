package com.kafkalite.network;

import com.kafkalite.core.log.LogManager;
import com.kafkalite.core.model.Record;
import com.kafkalite.network.protocol.*;

import java.nio.ByteBuffer;
import java.util.List;

public class ConnectionHandler {
    private final LogManager logManager;

    public ConnectionHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    public ByteBuffer handle(ByteBuffer frame) {
        short typeCode = frame.getShort();
        RequestType type = RequestType.fromCode(typeCode);
        try {
            return switch (type) {
                case PRODUCE -> handleProduce(frame);
                case FETCH -> handleFetch(frame);
                default -> throw new UnsupportedOperationException("Not implemented: " + type);
            };
        } catch (Exception e) {
            ByteBuffer err = ByteBuffer.allocate(256);
            err.put((byte) 1);
            ProtocolCodec.writeString(err, e.getMessage() == null ? "error" : e.getMessage());
            err.flip();
            return err;
        }
    }

    private ByteBuffer handleProduce(ByteBuffer frame) throws Exception {
        ProduceRequest req = ProduceRequest.decode(frame);
        long offset = logManager.getOrCreateLog(req.topic, req.partition).append(req.key, req.value);
        ByteBuffer resp = ByteBuffer.allocate(9);
        resp.put((byte) 0);
        resp.putLong(offset);
        resp.flip();
        return resp;
    }

    private ByteBuffer handleFetch(ByteBuffer frame) throws Exception {
        FetchRequest req = FetchRequest.decode(frame);
        var log = logManager.getLog(req.topic, req.partition);
        List<Record> records = log == null ? List.of() : log.read(req.fetchOffset, req.maxRecords);

        int size = 5;
        for (Record r : records) size += r.sizeOnDisk();
        ByteBuffer resp = ByteBuffer.allocate(size);
        resp.put((byte) 0);
        resp.putInt(records.size());
        for (Record r : records) resp.put(r.serialize());
        resp.flip();
        return resp;
    }
}
