package org.borer.logdb.bit;

public interface Memory
{
    Object unsafeObject();

    long getBaseAddress();

    void resetPosition();

    long getCapacity();

    void putLong(long value);

    void putLong(long offset, long value);

    long getLong();

    long getLong(long offset);

    void putInt(int value);

    void putInt(long offset, int value);

    int getInt();

    int getInt(long offset);

    void getBytes(byte[] array);

    void getBytes(long sourceOffset, byte[] destinationArray);

    void putBytes(byte[] sourceArray);

    void putBytes(long destinationOffset, byte[] sourceArray);

    void putByte(byte b);

    byte getByte();

    byte getByte(long offset);

    void assertBounds(long requestOffset, int requestLength);

    void assertBounds(long requestOffset, long requestLength);
}
