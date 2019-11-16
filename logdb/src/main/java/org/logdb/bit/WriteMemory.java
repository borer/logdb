package org.logdb.bit;

import org.logdb.storage.ByteOffset;

public interface WriteMemory
{
    void putLong(long value);

    void putLong(@ByteOffset long offset, long value);

    void putInt(int value);

    void putInt(@ByteOffset long offset, int value);

    void putBytes(byte[] sourceArray);

    void putBytes(@ByteOffset long destinationOffset, byte[] sourceArray);

    void putByte(byte b);

    void putByte(@ByteOffset long offset, byte b);

    void reset();
}
