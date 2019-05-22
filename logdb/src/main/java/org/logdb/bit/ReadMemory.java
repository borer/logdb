package org.logdb.bit;

import java.nio.ByteBuffer;

public interface ReadMemory
{
    ByteBuffer getSupportByteBufferIfAny();

    long getBaseAddress();

    void resetPosition();

    long getCapacity();

    long getLong();

    long getLong(long offset);

    int getInt();

    int getInt(long offset);

    void getBytes(byte[] array);

    void getBytes(long sourceOffset, byte[] destinationArray);

    byte getByte();

    byte getByte(long offset);
}
