package org.logdb.logfile;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import java.nio.ByteBuffer;

import static org.logdb.storage.StorageUnits.CHAR_SIZE;
import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;
import static org.logdb.storage.StorageUnits.ZERO_SIZE;


public class LogRecordHeader
{
    private static final @ByteSize int CRC_SIZE = INT_BYTES_SIZE;
    private static final @ByteOffset int CRC_OFFSET = ZERO_OFFSET;

    private static final @ByteSize int TYPE_SIZE = CHAR_SIZE;
    private static final @ByteOffset int TYPE_OFFSET = CRC_OFFSET + StorageUnits.offset(CRC_SIZE);

    private static final @ByteSize int KEY_SIZE = INT_BYTES_SIZE;
    private static final @ByteOffset int KEY_OFFSET = TYPE_OFFSET + StorageUnits.offset(TYPE_SIZE);

    private static final @ByteSize int VALUE_SIZE = INT_BYTES_SIZE;
    private static final @ByteOffset int VALUE_OFFSET = KEY_OFFSET + StorageUnits.offset(KEY_SIZE);

    private static final @ByteSize int VERSION_SIZE = LONG_BYTES_SIZE;
    private static final @ByteOffset int VERSION_OFFSET = VALUE_OFFSET + StorageUnits.offset(VALUE_SIZE);

    private static final @ByteSize int TIMESTAMP_SIZE = LONG_BYTES_SIZE;
    private static final @ByteOffset int TIMESTAMP_OFFSET = VERSION_OFFSET + StorageUnits.offset(VERSION_SIZE);

    public static final @ByteSize int RECORD_HEADER_SIZE = CRC_SIZE + TYPE_SIZE + KEY_SIZE + VALUE_SIZE + VERSION_SIZE + TIMESTAMP_SIZE;

    private int checksum;
    private LogRecordType recordType;
    private @ByteSize int keyLength;
    private @ByteSize int valueLength;
    private @Version long version;
    private @Milliseconds long timestamp;

    public int getChecksum()
    {
        return checksum;
    }

    public LogRecordType getRecordType()
    {
        return recordType;
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

    public void initPut(final int checksum,
                        final @ByteSize int keyLength,
                        final @ByteSize int valueLength,
                        final @Version long version,
                        final @Milliseconds long timestamp)
    {
        init(checksum, LogRecordType.UPDATE, keyLength, valueLength, version, timestamp);
    }

    public void initDelete(final int checksum,
                           final @ByteSize int keyLength,
                           final @Version long version,
                           final @Milliseconds long timestamp)
    {
        init(checksum, LogRecordType.DELETE, keyLength, ZERO_SIZE, version, timestamp);
    }

    private void init(
            final int checksum,
            final LogRecordType recordType,
            final @ByteSize int keyLength,
            final @ByteSize int valueLength,
            final @Version long version,
            final @Milliseconds long timestamp)
    {
        this.checksum = checksum;
        this.recordType = recordType;
        this.keyLength = keyLength;
        this.valueLength = valueLength;
        this.version = version;
        this.timestamp = timestamp;
    }

    public void read(final ByteBuffer buffer)
    {
        buffer.rewind();
        this.checksum = buffer.getInt();
        this.recordType = LogRecordType.fromChar(buffer.getChar());
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
        destinationBuffer.putChar(TYPE_OFFSET, recordType.getChar());
        destinationBuffer.putInt(KEY_OFFSET, keyLength);
        destinationBuffer.putInt(VALUE_OFFSET, valueLength);
        destinationBuffer.putLong(VERSION_OFFSET, version);
        destinationBuffer.putLong(TIMESTAMP_OFFSET, timestamp);
        destinationBuffer.rewind();
    }
}
