package org.logdb.checksum;

import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.util.Arrays;

import static org.logdb.bit.BinaryHelper.longToBytes;

public class Fletcher32 extends Fletcher8
{
    private static final @ByteSize int SIZE = StorageUnits.size(32);

    @Override
    public byte[] getValue()
    {
        final byte[] bytes = new byte[SIZE];
        longToBytes(a, bytes, 0);
        longToBytes(b, bytes, 8);
        longToBytes(c, bytes, 16);
        longToBytes(d, bytes, 24);

        return bytes;
    }

    @Override
    public @ByteSize int getValueSize()
    {
        return SIZE;
    }

    @Override
    public boolean compare(final byte[] bytes)
    {
        final byte[] currentValue = getValue();
        return Arrays.equals(currentValue, bytes);
    }
}
