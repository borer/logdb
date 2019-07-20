package org.logdb.storage.file;

import org.logdb.storage.ByteOffset;

import java.nio.MappedByteBuffer;

public class MappedBuffer
{
    final MappedByteBuffer buffer;
    final @ByteOffset long address;

    public MappedBuffer(final MappedByteBuffer buffer, final @ByteOffset long address)
    {
        this.buffer = buffer;
        this.address = address;
    }

    @Override
    public String toString()
    {
        return "MappedBuffer{" +
                "buffer=" + buffer +
                ", address=" + address +
                '}';
    }
}
