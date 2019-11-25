package org.logdb.checksum;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import static org.logdb.checksum.BinaryHelper.longToBytes;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class ChecksumHelper
{
    private final Checksum checksum;
    private byte[] valueBuffer;
    private final ChecksumType checksumType;

    public ChecksumHelper(final Checksum checksum, final ChecksumType checksumType)
    {
        this.checksum = checksum;
        this.valueBuffer = new byte[checksum.getValueSize()];
        this.checksumType = checksumType;
    }

    public byte[] calculateSingleChecksum(final byte[] buffer, final @ByteOffset int offset, final @ByteSize int length)
    {
        checksum.reset();
        checksum.update(buffer, offset, length);

        return checksum.getValue();
    }

    public boolean compareSingleChecksum(final byte[] originalChecksum,
                                         final byte[] buffer,
                                         final int offset,
                                         final int length)
    {
        checksum.reset();
        checksum.update(buffer, offset, length);

        return checksum.compare(originalChecksum);
    }

    public void updateChecksum(final byte[] buffer, final @ByteOffset int offset, final @ByteSize int length)
    {
        checksum.update(buffer, offset, length);
    }

    public void updateChecksum(final long value)
    {
        longToBytes(value, valueBuffer);
        checksum.update(valueBuffer, ZERO_OFFSET, valueBuffer.length);
    }

    public byte[] getAndResetChecksum()
    {
        final byte[] value = checksum.getValue();
        checksum.reset();

        return value;
    }

    public ChecksumType getType()
    {
        return checksumType;
    }

    public @ByteSize int getValueSize()
    {
        return StorageUnits.size(valueBuffer.length);
    }
}
