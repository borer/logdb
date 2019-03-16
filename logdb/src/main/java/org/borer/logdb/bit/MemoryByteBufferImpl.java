package org.borer.logdb.bit;

import java.nio.ByteBuffer;

public class MemoryByteBufferImpl implements Memory
{
    private final ByteBuffer buffer;

    MemoryByteBufferImpl(final ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public void resetPosition()
    {
        buffer.position(0);
    }

    @Override
    public void putLong(final long value)
    {
        buffer.putLong(value);
    }

    @Override
    public void putLong(final long offset, final long value)
    {
        buffer.putLong((int)offset, value);
    }

    @Override
    public long getLong()
    {
        return buffer.getLong();
    }

    @Override
    public long getLong(final long offset)
    {
        return buffer.getLong((int)offset);
    }

    @Override
    public void putInt(final int value)
    {
        buffer.putInt(value);
    }

    @Override
    public void putInt(final long offset, final int value)
    {
        buffer.putInt((int)offset, value);
    }

    @Override
    public int getInt()
    {
        return buffer.getInt();
    }

    @Override
    public int getInt(final long offset)
    {
        return buffer.getInt((int)offset);
    }

    @Override
    public void getBytes(final byte[] array)
    {
        buffer.get(array);
    }

    @Override
    public void getBytes(long sourceOffset, byte[] destinationArray)
    {
        for (int i = 0; i < destinationArray.length; i++)
        {
            destinationArray[i] = buffer.get((int)sourceOffset + i);
        }
    }

    @Override
    public void putBytes(byte[] sourceArray)
    {
        buffer.put(sourceArray);
    }

    @Override
    public void putBytes(long destinationOffset, byte[] sourceArray)
    {
        for (int i = 0; i < sourceArray.length; i++)
        {
            buffer.put((int)destinationOffset + i, sourceArray[i]);
        }
    }
}
