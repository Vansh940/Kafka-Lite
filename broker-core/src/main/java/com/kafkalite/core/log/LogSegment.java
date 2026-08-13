package com.kafkalite.core.log;

import com.kafkalite.core.model.Record;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** One physical (.log + .index) file pair covering a contiguous offset range within a partition. */
public class LogSegment {
    public final long baseOffset;
    private final FileChannel channel;
    private final OffsetIndex index;
    private long nextOffset;
    private long sizeBytes;

    public LogSegment(Path dir, long baseOffset) throws IOException {
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;
        Path logPath = dir.resolve(String.format("%020d.log", baseOffset));
        Path indexPath = dir.resolve(String.format("%020d.index", baseOffset));
        RandomAccessFile raf = new RandomAccessFile(logPath.toFile(), "rw");
        this.channel = raf.getChannel();
        this.sizeBytes = channel.size();
        this.channel.position(sizeBytes);
        this.index = new OffsetIndex(indexPath);
    }

    public synchronized long append(byte[] key, byte[] value, long timestamp) throws IOException {
        long offset = nextOffset;
        Record record = new Record(offset, key, value, timestamp);
        byte[] serialized = record.serialize();
        long position = channel.position();
        channel.write(ByteBuffer.wrap(serialized));
        index.maybeAppend(offset, position, serialized.length);
        sizeBytes += serialized.length;
        nextOffset++;
        return offset;
    }

    /** Reads up to maxRecords starting at fromOffset (inclusive), scanning forward via the sparse index. */
    public synchronized List<Record> read(long fromOffset, int maxRecords) throws IOException {
        List<Record> result = new ArrayList<>();
        long[] indexed = index.lookup(fromOffset);
        long pos = indexed[1];

        while (result.size() < maxRecords && pos < channel.size()) {
            ByteBuffer header = ByteBuffer.allocate(16); // offset(8) + length(4) + crc(4)
            int read = channel.read(header, pos);
            if (read < 16) break;
            header.flip();
            header.mark();
            long offset = header.getLong();
            int length = header.getInt();
            header.reset();

            ByteBuffer full = ByteBuffer.allocate(16 + length);
            channel.read(full, pos);
            full.flip();

            if (offset >= fromOffset) {
                result.add(Record.deserialize(full));
            }
            pos += 16 + length;
        }
        return result;
    }

    public long size() { return sizeBytes; }
    public long nextOffset() { return nextOffset; }
    public void flush() throws IOException { channel.force(true); }
    public void close() throws IOException { channel.close(); }
}
