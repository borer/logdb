package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;

public interface ReadMemory
{
    long getLong();

    long getLong(@ByteOffset long offset);

    int getInt();

    int getInt(@ByteOffset long offset);

    short getShort(@ByteOffset long offset);

    void getBytes(byte[] destinationArray);

    void getBytes(@ByteSize long length, byte[] destinationArray);

    void getBytes(@ByteOffset long offset, @ByteSize long length, byte[] destinationArray);

    void getBytes(@ByteOffset long offset, @ByteSize long length, byte[] destinationArray, @ByteOffset long destinationArrayOffset);

    byte getByte();

    byte getByte(@ByteOffset long offset);
}
