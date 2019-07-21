package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;

import java.util.zip.CRC32;

import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class ChecksumUtil
{
    private final CRC32 checksumer = new CRC32();
    private byte[] longToBytesBuffer = new byte[Long.BYTES];

    public int calculateSingleChecksum(final byte[] buffer, final @ByteOffset int offset, final @ByteSize int length)
    {
        checksumer.reset();
        checksumer.update(buffer, offset, length);

        return (int)checksumer.getValue();
    }

    public boolean compareSingleChecksum(final int originalChecksum,
                                         final byte[] buffer,
                                         final int offset,
                                         final int length)
    {
        checksumer.reset();
        checksumer.update(buffer, offset, length);

        final int calculated = (int)checksumer.getValue();

        return Integer.compareUnsigned(originalChecksum, calculated) == 0;
    }

    public void updateChecksum(final byte[] buffer, final @ByteOffset int offset, final @ByteSize int length)
    {
        checksumer.update(buffer, offset, length);
    }

    public void updateChecksum(final long value)
    {
        final byte[] bytes = longToBytes(value);
        checksumer.update(bytes, ZERO_OFFSET, bytes.length);
    }

    public int getAndResetChecksum()
    {
        final int value = (int) checksumer.getValue();
        checksumer.reset();

        return value;
    }

    private byte[] longToBytes(final long value)
    {
        long mutableValue = value;
        for (int i = longToBytesBuffer.length - 1; i >= 0; i--)
        {
            longToBytesBuffer[i] = (byte)(mutableValue & 0xFF);
            mutableValue >>= Byte.SIZE;
        }

        return longToBytesBuffer;
    }

    public boolean compareChecksum(final int checksum1, final int checksum2)
    {
        return Integer.compareUnsigned(checksum1, checksum2) == 0;
    }
}
