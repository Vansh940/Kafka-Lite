package com.kafkalite.core.log;

import com.kafkalite.core.model.Record;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Manages the ordered list of segments for ONE partition; rolls over at SEGMENT_MAX_BYTES. */
public class PartitionLog {
    private static final long SEGMENT_MAX_BYTES = 128L * 1024 * 1024; // 128MB, tune down for local testing

    private final Path dir;
    private final NavigableMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile long highWatermark = 0; // highest offset confirmed replicated to the ISR

    public PartitionLog(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
        segments.put(0L, new LogSegment(dir, 0));
    }

    public long append(byte[] key, byte[] value) throws IOException {
        lock.writeLock().lock();
        try {
            LogSegment active = segments.lastEntry().getValue();
            if (active.size() >= SEGMENT_MAX_BYTES) {
                active.flush();
                active = new LogSegment(dir, active.nextOffset());
                segments.put(active.baseOffset, active);
            }
            return active.append(key, value, System.currentTimeMillis());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Record> read(long fromOffset, int maxRecords) throws IOException {
        lock.readLock().lock();
        try {
            Long floorKey = segments.floorKey(fromOffset);
            if (floorKey == null) floorKey = segments.firstKey();
            return segments.get(floorKey).read(fromOffset, maxRecords);
        } finally {
            lock.readLock().unlock();
        }
    }

    public long logEndOffset() {
        lock.readLock().lock();
        try { return segments.lastEntry().getValue().nextOffset(); }
        finally { lock.readLock().unlock(); }
    }

    public long highWatermark() { return highWatermark; }
    public void advanceHighWatermark(long hw) { this.highWatermark = Math.max(this.highWatermark, hw); }

    public void flush() throws IOException {
        for (LogSegment s : segments.values()) s.flush();
    }
}
