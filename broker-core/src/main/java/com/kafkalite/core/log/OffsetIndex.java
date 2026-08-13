package com.kafkalite.core.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Sparse index: we do NOT index every offset (that would make the index as big
 * as the log itself). Roughly every INDEX_INTERVAL_BYTES of log data we record
 * one (offset -> filePosition) entry. A lookup binary-scans this small index,
 * then does a short linear scan in the log file itself.
 */
public class OffsetIndex {
    private static final int INDEX_INTERVAL_BYTES = 4096;
    private final List<long[]> entries = new ArrayList<>(); // [offset, position]
    private final Path indexPath;
    private long bytesSinceLastIndex = 0;

    public OffsetIndex(Path indexPath) throws IOException {
        this.indexPath = indexPath;
        load();
    }

    private void load() throws IOException {
        if (!indexPath.toFile().exists()) return;
        try (RandomAccessFile raf = new RandomAccessFile(indexPath.toFile(), "r")) {
            FileChannel ch = raf.getChannel();
            long len = ch.size();
            if (len == 0) return;
            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_ONLY, 0, len);
            while (buf.remaining() >= 16) {
                entries.add(new long[]{buf.getLong(), buf.getLong()});
            }
        }
    }

    public void maybeAppend(long offset, long position, int recordSize) throws IOException {
        bytesSinceLastIndex += recordSize;
        if (bytesSinceLastIndex >= INDEX_INTERVAL_BYTES || entries.isEmpty()) {
            entries.add(new long[]{offset, position});
            bytesSinceLastIndex = 0;
            try (RandomAccessFile raf = new RandomAccessFile(indexPath.toFile(), "rw")) {
                raf.seek(raf.length());
                raf.writeLong(offset);
                raf.writeLong(position);
            }
        }
    }

    /** Largest indexed (offset, position) pair with offset <= target; {0,0} if nothing indexed yet. */
    public long[] lookup(long targetOffset) {
        long[] best = {0, 0};
        for (long[] entry : entries) {
            if (entry[0] <= targetOffset) best = entry;
            else break;
        }
        return best;
    }
}
