package org.logdb.logfile;

import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import java.nio.ByteBuffer;

import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;


class LogRecordHeader
{
    private static final @ByteSize int CRC_SIZE = INT_BYTES_SIZE;
    private static final int CRC_OFFSET = 0;

    private static final @ByteSize int KEY_SIZE = INT_BYTES_SIZE;
    private static final int KEY_OFFSET = CRC_OFFSET + CRC_SIZE;

    private static final @ByteSize int VALUE_SIZE = INT_BYTES_SIZE;
    private static final int VALUE_OFFSET = KEY_OFFSET + KEY_SIZE;

    private static final @ByteSize int VERSION_SIZE = LONG_BYTES_SIZE;
    private static final int VERSION_OFFSET = VALUE_OFFSET + VALUE_SIZE;

    private static final @ByteSize int TIMESTAMP_SIZE = LONG_BYTES_SIZE;
    private static final int TIMESTAMP_OFFSET = VERSION_OFFSET + VERSION_SIZE;

    static final @ByteSize int RECORD_HEADER_SIZE = CRC_SIZE + KEY_SIZE + VALUE_SIZE + VERSION_SIZE + TIMESTAMP_SIZE;

    private int checksum;
    private @ByteSize int keyLength;
    private @ByteSize int valueLength;
    private @Version long version;
    private @Milliseconds long timestamp;

    public int getChecksum()
    {
        return checksum;
    }

    public @ByteSize int getKeyLength()
    {
        return keyLength;
    }

    public @ByteSize int getValueLength()
    {
        return valueLength;
    }

    public @Version long getVersion()
    {
        return version;
    }

    public @Milliseconds long getTimestamp()
    {
        return timestamp;
    }

    public void init(
            final int checksum,
            final @ByteSize int keyLength,
            final @ByteSize int valueLength,
            final @Version long version,
            final @Milliseconds long timestamp)
    {
        this.checksum = checksum;
        this.keyLength = keyLength;
        this.valueLength = valueLength;
        this.version = version;
        this.timestamp = timestamp;
    }

    public void read(final ByteBuffer buffer)
    {
        buffer.rewind();
        this.checksum = buffer.getInt();
        this.keyLength = StorageUnits.size(buffer.getInt());
        this.valueLength = StorageUnits.size(buffer.getInt());
        this.version = StorageUnits.version(buffer.getLong());
        this.timestamp = TimeUnits.millis(buffer.getLong());
        buffer.rewind();
    }

    public void write(final ByteBuffer destinationBuffer)
    {
        destinationBuffer.rewind();
        destinationBuffer.putInt(CRC_OFFSET, checksum);
        destinationBuffer.putInt(KEY_OFFSET, keyLength);
        destinationBuffer.putInt(VALUE_OFFSET, valueLength);
        destinationBuffer.putLong(VERSION_OFFSET, version);
        destinationBuffer.putLong(TIMESTAMP_OFFSET, timestamp);
        destinationBuffer.rewind();
    }
}
