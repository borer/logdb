package org.logdb.root.index;

import org.logdb.bit.DirectMemory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class RootIndexRecord
{
    private static final @ByteSize int VERSION_SIZE = LONG_BYTES_SIZE;
    private static final @ByteOffset int VERSION_OFFSET = ZERO_OFFSET;

    private static final @ByteSize int TIMESTAMP_SIZE = LONG_BYTES_SIZE;
    private static final @ByteOffset int TIMESTAMP_OFFSET = VERSION_OFFSET + StorageUnits.offset(VERSION_SIZE);

    private static final @ByteSize int OFFSET_VALUE_SIZE = LONG_BYTES_SIZE;
    private static final @ByteOffset int OFFSET_VALUE_OFFSET = TIMESTAMP_OFFSET + StorageUnits.offset(TIMESTAMP_SIZE);

    static final @ByteSize int SIZE = VERSION_SIZE + TIMESTAMP_SIZE + OFFSET_VALUE_SIZE;

    private final ByteBuffer recordBuffer;

    RootIndexRecord(final ByteOrder order,
                    final @Version long version,
                    final @Milliseconds long timestamp,
                    final @ByteOffset long offsetValue)
    {
        this.recordBuffer = ByteBuffer.allocate(SIZE);
        recordBuffer.order(order);
        set(version, timestamp, offsetValue);
    }

    public static RootIndexRecord read(final DirectMemory directMemory, final @ByteOffset long offset)
    {
        final @Version long version = readVersion(directMemory, offset);
        final @Milliseconds long timestamp = readTimestamp(directMemory, offset);
        final @ByteOffset long offsetValue = readOffsetValue(directMemory, offset);

        return new RootIndexRecord(directMemory.getByteOrder(), version, timestamp, offsetValue);
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

    public @Version long getVersion()
    {
        return StorageUnits.version(recordBuffer.getLong(VERSION_OFFSET));
    }

    public @Milliseconds long getTimestamp()
    {
        return TimeUnits.millis(recordBuffer.getLong(TIMESTAMP_OFFSET));
    }


    public @ByteOffset long getOffset()
    {
        return StorageUnits.offset(recordBuffer.getLong(OFFSET_VALUE_OFFSET));
    }

    public ByteBuffer getBuffer()
    {
        return recordBuffer;
    }

    static @ByteOffset long readOffsetValue(final DirectMemory directMemory, final @ByteOffset long recordOffset)
    {
        final @ByteOffset long offsetOffset = offsetValueOffset(recordOffset);
        return StorageUnits.offset(directMemory.getLong(offsetOffset));
    }

    static @Milliseconds long readTimestamp(final DirectMemory directMemory, final @ByteOffset long recordOffset)
    {
        final @ByteOffset long timestampOffset = timestampOffset(recordOffset);
        return TimeUnits.millis(directMemory.getLong(timestampOffset));
    }

    static @Version long readVersion(final DirectMemory directMemory, final @ByteOffset long recordOffset)
    {
        final @ByteOffset long versionOffset = versionOffset(recordOffset);
        return StorageUnits.version(directMemory.getLong(versionOffset));
    }

    public static @ByteOffset long versionOffset(final @ByteOffset long baseOffset)
    {
        return baseOffset + VERSION_OFFSET;
    }

    public static @ByteOffset long timestampOffset(final @ByteOffset long baseOffset)
    {
        return baseOffset + TIMESTAMP_OFFSET;
    }

    public static @ByteOffset long offsetValueOffset(final @ByteOffset long baseOffset)
    {
        return baseOffset + OFFSET_VALUE_OFFSET;
    }
}
