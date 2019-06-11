package org.logdb.bit;

import org.logdb.storage.ByteOffset;

import java.nio.ByteBuffer;

public interface ReadMemory
{
    ByteBuffer getSupportByteBufferIfAny();

    long getLong();

    long getLong(@ByteOffset long offset);

    int getInt();

    int getInt(@ByteOffset long offset);

    void getBytes(byte[] destinationArray);

    void getBytes(@ByteOffset long length, byte[] destinationArray);

    void getBytes(@ByteOffset long offset, long length, byte[] destinationArray);

    void getBytes(@ByteOffset long offset, long length, byte[] destinationArray, @ByteOffset long destinationArrayOffset);

    byte getByte();

    byte getByte(@ByteOffset long offset);
}
