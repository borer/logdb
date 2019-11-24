package org.logdb.checksum;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class ChecksumUtil
{
    private final Checksum checksumer = new CRC32();
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
        longToBytes(value, longToBytesBuffer);
        checksumer.update(longToBytesBuffer, ZERO_OFFSET, longToBytesBuffer.length);
    }

    public int getAndResetChecksum()
    {
        final int value = (int) checksumer.getValue();
        checksumer.reset();

        return value;
    }

    public boolean compareChecksum(final int checksum1, final int checksum2)
    {
        return Integer.compareUnsigned(checksum1, checksum2) == 0;
    }

    static void longToBytes(final long value, final byte[] bytes)
    {
        bytes[0] = (byte)value;
        bytes[1] = (byte)(value >> Byte.SIZE);
        bytes[2] = (byte)(value >> 16);
        bytes[3] = (byte)(value >> 24);
        bytes[4] = (byte)(value >> 32);
        bytes[5] = (byte)(value >> 40);
        bytes[6] = (byte)(value >> 48);
        bytes[7] = (byte)(value >> 56);
    }
}
