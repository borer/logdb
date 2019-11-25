package org.logdb.logfile;

import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.logdb.storage.StorageUnits.CHAR_SIZE;
import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_SIZE;


public class LogRecordHeader
{
    private static final @ByteSize int TYPE_SIZE = CHAR_SIZE;
    private static final @ByteSize int KEY_SIZE = INT_BYTES_SIZE;
    private static final @ByteSize int VALUE_SIZE = INT_BYTES_SIZE;
    private static final @ByteSize int VERSION_SIZE = LONG_BYTES_SIZE;
    private static final @ByteSize int TIMESTAMP_SIZE = LONG_BYTES_SIZE;
    public static final @ByteSize int RECORD_HEADER_STATIC_SIZE = TYPE_SIZE + KEY_SIZE + VALUE_SIZE + VERSION_SIZE + TIMESTAMP_SIZE;

    private final @ByteSize int checksumSize;
    private byte[] checksum;
    private LogRecordType recordType;
    private @ByteSize int keyLength;
    private @ByteSize int valueLength;
    private @Version long version;
    private @Milliseconds long timestamp;

    public LogRecordHeader(final @ByteSize int checksumSize)
    {
        this.checksumSize = checksumSize;
    }

    public byte[] getChecksum()
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

    void initPut(final byte[] checksum,
                 final @ByteSize int keyLength,
                 final @ByteSize int valueLength,
                 final @Version long version,
                 final @Milliseconds long timestamp)
    {
        init(checksum, LogRecordType.UPDATE, keyLength, valueLength, version, timestamp);
    }

    void initDelete(final byte[] checksum,
                    final @ByteSize int keyLength,
                    final @Version long version,
                    final @Milliseconds long timestamp)
    {
        init(checksum, LogRecordType.DELETE, keyLength, ZERO_SIZE, version, timestamp);
    }

    private void init(
            final byte[] checksum,
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
        this.checksum = new byte[checksumSize];
        buffer.get(checksum);
        this.recordType = LogRecordType.fromChar(buffer.getChar());
        this.keyLength = StorageUnits.size(buffer.getInt());
        this.valueLength = StorageUnits.size(buffer.getInt());
        this.version = StorageUnits.version(buffer.getLong());
        this.timestamp = TimeUnits.millis(buffer.getLong());
        buffer.rewind();
    }

    void write(final ByteBuffer destinationBuffer)
    {
        destinationBuffer.rewind();
        destinationBuffer.put(checksum);
        destinationBuffer.putChar(recordType.getChar());
        destinationBuffer.putInt(keyLength);
        destinationBuffer.putInt(valueLength);
        destinationBuffer.putLong(version);
        destinationBuffer.putLong(timestamp);
        destinationBuffer.rewind();
    }

    public @ByteSize int getSize()
    {
        return StorageUnits.size(RECORD_HEADER_STATIC_SIZE + checksumSize);
    }

    @Override
    public String toString()
    {
        return "LogRecordHeader{" +
                "checksum=" + Arrays.toString(checksum) +
                ", recordType=" + recordType +
                ", keyLength=" + keyLength +
                ", valueLength=" + valueLength +
                ", version=" + version +
                ", timestamp=" + timestamp +
                '}';
    }
}
