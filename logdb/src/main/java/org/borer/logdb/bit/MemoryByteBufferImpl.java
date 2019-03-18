package org.borer.logdb.bit;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;

public class MemoryByteBufferImpl implements Memory
{
    private final ByteBuffer buffer;

    MemoryByteBufferImpl(final ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public Object unsafeObject()
    {
        return buffer.array();
    }

    @Override
    public long getBaseAddress()
    {
        return Unsafe.ARRAY_BYTE_BASE_OFFSET;
    }

    @Override
    public void resetPosition()
    {
        buffer.position(0);
    }

    @Override
    public long getCapacity()
    {
        return buffer.capacity();
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

    @Override
    public void putByte(final byte b)
    {
        buffer.put(b);
    }

    @Override
    public byte getByte()
    {
        return buffer.get();
    }

    @Override
    public byte getByte(final long offset)
    {
        return buffer.get((int) offset);
    }

    @Override
    public void assertBounds(long requestOffset, int requestLength)
    {
        assert ((requestOffset | requestLength | (requestOffset + requestLength)
                | (buffer.capacity() - (requestOffset + requestLength))) >= 0) :
                "requestOffset: " + requestOffset + ", requestLength: " + requestLength
                        + ", (requestOffset + requestLength): " + (requestOffset + requestLength)
                        + ", allocSize: " + buffer.capacity();
    }

    @Override
    public void assertBounds(long requestOffset, long requestLength)
    {
        assert ((requestOffset | requestLength | (requestOffset + requestLength)
                | (buffer.capacity() - (requestOffset + requestLength))) >= 0) :
                "requestOffset: " + requestOffset + ", requestLength: " + requestLength
                        + ", (requestOffset + requestLength): " + (requestOffset + requestLength)
                        + ", allocSize: " + buffer.capacity();
    }
}
