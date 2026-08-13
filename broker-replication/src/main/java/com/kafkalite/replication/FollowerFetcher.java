package com.kafkalite.replication;

import com.kafkalite.core.log.LogManager;
import com.kafkalite.core.model.Record;
import com.kafkalite.network.protocol.FetchRequest;
import com.kafkalite.network.protocol.ProtocolCodec;
import com.kafkalite.network.protocol.RequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs on a FOLLOWER broker: repeatedly issues FETCH requests to the leader
 * for one partition and appends whatever comes back to the local replica log.
 * A replica fetch is structurally identical to a consumer fetch — same protocol.
 */
public class FollowerFetcher {
    private static final Logger log = LoggerFactory.getLogger(FollowerFetcher.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final String topic;
    private final int partition;
    private final LogManager logManager;
    private final String leaderHost;
    private final int leaderPort;
    private Socket socket;

    public FollowerFetcher(String topic, int partition, LogManager logManager, String leaderHost, int leaderPort) {
        this.topic = topic; this.partition = partition; this.logManager = logManager;
        this.leaderHost = leaderHost; this.leaderPort = leaderPort;
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::fetchOnce, 0, 50, TimeUnit.MILLISECONDS);
    }

    private void fetchOnce() {
        try {
            if (socket == null || socket.isClosed()) socket = new Socket(leaderHost, leaderPort);
            var localLog = logManager.getOrCreateLog(topic, partition);
            long nextOffset = localLog.logEndOffset();

            FetchRequest req = new FetchRequest(topic, partition, nextOffset, 500);
            ByteBuffer framed = ProtocolCodec.frame(RequestType.FETCH.code, req.encodeBody());
            socket.getOutputStream().write(framed.array(), 0, framed.limit());

            byte[] lenBuf = new byte[4];
            readFully(lenBuf);
            int respLen = ByteBuffer.wrap(lenBuf).getInt();
            byte[] respBuf = new byte[respLen];
            readFully(respBuf);

            ByteBuffer resp = ByteBuffer.wrap(respBuf);
            byte status = resp.get();
            if (status != 0) return;
            int count = resp.getInt();
            for (int i = 0; i < count; i++) {
                Record r = Record.deserialize(resp);
                localLog.append(r.key, r.value); // re-appends locally; offsets should align since fetch starts at logEndOffset
            }
        } catch (Exception e) {
            log.warn("Follower fetch failed for {}-{}, will retry", topic, partition, e);
            socket = null; // force reconnect next tick
        }
    }

    private void readFully(byte[] buf) throws Exception {
        int total = 0;
        while (total < buf.length) {
            int n = socket.getInputStream().read(buf, total, buf.length - total);
            if (n < 0) throw new java.io.IOException("Leader connection closed");
            total += n;
        }
    }

    public void stop() { scheduler.shutdownNow(); }
}
