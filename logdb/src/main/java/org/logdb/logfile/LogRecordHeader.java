package org.logdb.logfile;

import java.nio.ByteBuffer;

class LogRecordHeader
{
    private static final int CRC_SIZE = Integer.BYTES;
    private static final int CRC_OFFSET = 0;

    private static final int KEY_SIZE = Integer.BYTES;
    private static final int KEY_OFFSET = CRC_OFFSET + CRC_SIZE;

    private static final int VALUE_SIZE = Integer.BYTES;
    private static final int VALUE_OFFSET = KEY_OFFSET + KEY_SIZE;

    private static final int VERSION_SIZE = Long.BYTES;
    private static final int VERSION_OFFSET = VALUE_OFFSET + VALUE_SIZE;

    private static final int TIMESTAMP_SIZE = Long.BYTES;
    private static final int TIMESTAMP_OFFSET = VERSION_OFFSET + VERSION_SIZE;

    static final int RECORD_HEADER_SIZE = CRC_SIZE + KEY_SIZE + VALUE_SIZE + VERSION_SIZE + TIMESTAMP_SIZE;

    private int checksum;
    private int keyLength;
    private int valueLength;
    private long version;
    private long timestamp;

    public int getChecksum()
    {
        return checksum;
    }

    public int getKeyLength()
    {
        return keyLength;
    }

    public int getValueLength()
    {
        return valueLength;
    }

    public long getVersion()
    {
        return version;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void init(
            final int checksum,
            final int keyLength,
            final int valueLength,
            final long version,
            final long timestamp)
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
        this.keyLength = buffer.getInt();
        this.valueLength = buffer.getInt();
        this.version = buffer.getLong();
        this.timestamp = buffer.getLong();
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
