package org.logdb.root.index;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.nio.ByteBuffer;

import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class RootIndexRecord
{
    private static final @ByteSize int VERSION_SIZE = LONG_BYTES_SIZE;
    private static final @ByteOffset int VERSION_OFFSET = ZERO_OFFSET;

    private static final @ByteSize int TIMESTAMP_SIZE = LONG_BYTES_SIZE;
    private static final @ByteOffset int TIMESTAMP_OFFSET = VERSION_OFFSET + StorageUnits.offset(VERSION_SIZE);

    private static final @ByteSize int OFFSET_SIZE = LONG_BYTES_SIZE;
    private static final @ByteOffset int OFFSET_OFFSET = TIMESTAMP_OFFSET + StorageUnits.offset(TIMESTAMP_SIZE);

    private static final @ByteSize int SIZE = VERSION_SIZE + TIMESTAMP_SIZE + OFFSET_SIZE;

    private final ByteBuffer recordBuffer;

    RootIndexRecord()
    {
        this.recordBuffer = ByteBuffer.allocate(SIZE);
    }

    void set(
            final @Version long version,
            final @Milliseconds long timestamp,
            final @ByteOffset long offset)
    {
        recordBuffer.putLong(version);
        recordBuffer.putLong(timestamp);
        recordBuffer.putLong(offset);
        recordBuffer.rewind();
    }

    public ByteBuffer getBuffer()
    {
        return recordBuffer;
    }

    public static @ByteOffset long versionOffset(final @ByteOffset long baseOffset)
    {
        return baseOffset + VERSION_OFFSET;
    }

    public static @ByteOffset long timestampOffset(final @ByteOffset long baseOffset)
    {
        return baseOffset + TIMESTAMP_OFFSET;
    }

    public static @ByteOffset long offsetOffset(final @ByteOffset long baseOffset)
    {
        return baseOffset + OFFSET_OFFSET;
    }
}
