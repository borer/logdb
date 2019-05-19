package org.logdb.bit;

public interface WriteMemory
{
    long getBaseAddress();

    void resetPosition();

    long getCapacity();

    void putLong(long value);

    void putLong(long offset, long value);

    void putInt(int value);

    void putInt(long offset, int value);

    void putBytes(byte[] sourceArray);

    void putBytes(long destinationOffset, byte[] sourceArray);

    void putByte(byte b);

    void putByte(long offset, byte b);

    void assertBounds(long requestOffset, int requestLength);

    void assertBounds(long requestOffset, long requestLength);
}
