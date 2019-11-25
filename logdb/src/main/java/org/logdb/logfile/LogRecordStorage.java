package org.logdb.logfile;

import org.logdb.checksum.ChecksumHelper;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

/**
 * The structure of the LogRecordStorage on disc is
 * ------------------------------------------ 0
 * |                 Header                 |
 * ------------------------------------------ h
 * |                 Key                    |
 * ------------------------------------------ h + keyLength
 * |                 Value                  |
 * ------------------------------------------ h + keyLength + valueLength (N)
 * |                 ....                   |
 * ------------------------------------------ N + 1
 */
class LogRecordStorage
{
    private final ChecksumHelper checksumHelper;
    private final ByteBuffer headerBuffer;
    private final Storage storage;
    private final LogRecordHeader logRecordHeader;

    LogRecordStorage(final Storage storage, final ChecksumHelper checksumHelper)
    {
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null");
        this.checksumHelper = Objects.requireNonNull(checksumHelper, "checkHelper cannot be null");

        this.logRecordHeader = new LogRecordHeader(checksumHelper.getValueSize());

        this.headerBuffer = ByteBuffer.allocate(LogRecordHeader.RECORD_HEADER_STATIC_SIZE + checksumHelper.getValueSize());
        headerBuffer.order(storage.getOrder());
    }

    @ByteOffset long writePut(
            final byte[] key,
            final byte[] value,
            final @Version long version,
            final @Milliseconds long timestamp) throws IOException
    {
        final byte[] checksum = calculatePutChecksum(key, value, version, timestamp);
        logRecordHeader.initPut(
                checksum,
                StorageUnits.size(key.length),
                StorageUnits.size(value.length),
                version,
                timestamp);
        logRecordHeader.write(headerBuffer);

        final @ByteOffset long positionOffset = storage.append(headerBuffer);
        storage.append(ByteBuffer.wrap(key));
        storage.append(ByteBuffer.wrap(value));

        return positionOffset;
    }

    byte[] readRecordValue(final @ByteOffset long offset)
    {
        readHeader(offset);

        if (LogRecordType.UPDATE == logRecordHeader.getRecordType())
        {
            final @ByteOffset long valueOffset = StorageUnits.offset(offset + logRecordHeader.getSize() + logRecordHeader.getKeyLength());
            return readValue(valueOffset);
        }
        else if (LogRecordType.DELETE == logRecordHeader.getRecordType())
        {
            throw new IllegalArgumentException("offset " + offset + " refers to a delete record");
        }
        else
        {
            throw new IllegalArgumentException("offset " + offset + " refers to a invalid record ");
        }
    }

    @ByteOffset long writeDelete(
            final byte[] key,
            final @Version long version,
            final @Milliseconds long timestamp) throws IOException
    {
        final byte[] checksum = calculateDeleteChecksum(key, version, timestamp);
        logRecordHeader.initDelete(
                checksum,
                StorageUnits.size(key.length),
                version,
                timestamp);
        logRecordHeader.write(headerBuffer);

        final @ByteOffset long offset = storage.append(headerBuffer);
        storage.append(ByteBuffer.wrap(key));

        return offset;
    }

    private byte[] calculateDeleteChecksum(
            final byte[] key,
            final @Version long version,
            final @Milliseconds long timestamp)
    {
        checksumHelper.updateChecksum(key, ZERO_OFFSET, StorageUnits.size(key.length));
        checksumHelper.updateChecksum(version);
        checksumHelper.updateChecksum(timestamp);
        return checksumHelper.getAndResetChecksum();
    }

    private byte[] calculatePutChecksum(
            final byte[] key,
            final byte[] value,
            final @Version long version,
            final @Milliseconds long timestamp)
    {
        checksumHelper.updateChecksum(key, ZERO_OFFSET, StorageUnits.size(key.length));
        checksumHelper.updateChecksum(value, ZERO_OFFSET, StorageUnits.size(value.length));
        checksumHelper.updateChecksum(version);
        checksumHelper.updateChecksum(timestamp);
        return checksumHelper.getAndResetChecksum();
    }

    private void readHeader(final @ByteOffset long offset)
    {
        try
        {
            final ByteBuffer headerBuffer = ByteBuffer.allocate(logRecordHeader.getSize());
            headerBuffer.order(storage.getOrder());
            storage.readBytes(offset, headerBuffer);

            logRecordHeader.read(headerBuffer);
        }
        catch (final RuntimeException e)
        {
            throw new IllegalStateException("Unable to read record header at offset " + offset, e);
        }
    }

    private byte[] readValue(final @ByteOffset long offset)
    {
        final ByteBuffer valueBuffer = ByteBuffer.allocate(logRecordHeader.getValueLength());
        valueBuffer.order(storage.getOrder());

        storage.readBytes(offset, valueBuffer);

        return valueBuffer.array();
    }
}
