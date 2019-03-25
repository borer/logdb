package org.borer.logdb.bit;

public class MemoryDirectImpl implements Memory
{
    private final long baseAddress;
    private final long capacity;

    private long position;

    MemoryDirectImpl(final long baseAddress, final long capacity)
    {
        this.baseAddress = baseAddress;
        this.capacity = capacity;
        this.position = 0;
    }

    @Override
    public byte[] getSupportByteArrayIfAny()
    {
        return null;
    }

    @Override
    public long getBaseAddress()
    {
        return baseAddress;
    }

    @Override
    public void resetPosition()
    {
        position = 0;
    }

    @Override
    public long getCapacity()
    {
        return capacity;
    }

    @Override
    public void putLong(final long value)
    {
        assertBounds(position, Long.BYTES);

        putLong(position, value);
        position += Long.BYTES;
    }

    @Override
    public void putLong(final long offset, final long value)
    {
        assertBounds(offset, Long.BYTES);

        NativeMemoryAccess.putLong(baseAddress + offset, value);
    }

    @Override
    public long getLong()
    {
        return getLong(position);
    }

    @Override
    public long getLong(final long offset)
    {
        assertBounds(offset, Long.BYTES);

        return NativeMemoryAccess.getLong(baseAddress + offset);
    }

    @Override
    public void putInt(final int value)
    {
        assertBounds(position, Integer.BYTES);

        putInt(position, value);
        position += Integer.BYTES;
    }

    @Override
    public void putInt(final long offset, final int value)
    {
        assertBounds(offset, Integer.BYTES);

        NativeMemoryAccess.putInt(baseAddress + offset, value);
    }

    @Override
    public int getInt()
    {
        return getInt(position);
    }

    @Override
    public int getInt(final long offset)
    {
        assertBounds(offset, Integer.BYTES);

        return NativeMemoryAccess.getInt(baseAddress + offset);
    }

    @Override
    public void getBytes(final byte[] array)
    {
        getBytes(position, array);
    }

    @Override
    public void getBytes(final long sourceOffset, final byte[] destinationArray)
    {
        assertBounds(sourceOffset, destinationArray.length);

        NativeMemoryAccess.getBytes(baseAddress + sourceOffset, destinationArray);
    }

    @Override
    public void putBytes(final byte[] sourceArray)
    {
        assertBounds(position, sourceArray.length);

        putBytes(position, sourceArray);
        position += sourceArray.length;
    }

    @Override
    public void putBytes(final long destinationOffset, final byte[] sourceArray)
    {
        assertBounds(destinationOffset, sourceArray.length);

        NativeMemoryAccess.putBytes(baseAddress + destinationOffset, sourceArray);
    }

    @Override
    public void putByte(final byte b)
    {
        putByte(position, b);
        position += Byte.BYTES;
    }

    @Override
    public void putByte(final long offset, final byte b)
    {
        assertBounds(offset, Byte.BYTES);

        NativeMemoryAccess.putByte(baseAddress + offset, b);
    }

    @Override
    public byte getByte()
    {
        return getByte(position);
    }

    @Override
    public byte getByte(final long offset)
    {
        assertBounds(baseAddress + offset, Byte.BYTES);

        return NativeMemoryAccess.getByte(baseAddress + offset);
    }

    @Override
    public void assertBounds(final long requestOffset, final int requestLength)
    {
        assert ((requestOffset
                | requestLength
                | (requestOffset + requestLength)
                | (capacity - (requestOffset + requestLength - baseAddress))) >= 0) :
                "requestOffset: " + requestOffset + ", requestLength: " + requestLength
                 + ", (requestOffset + requestLength): " + (requestOffset + requestLength) + ", allocSize: " + capacity;
    }

    @Override
    public void assertBounds(final long requestOffset, final long requestLength)
    {
        assert ((requestOffset
                | requestLength
                | (requestOffset + requestLength)
                | (capacity - (requestOffset + requestLength - baseAddress))) >= 0) :
                "requestOffset: " + requestOffset + ", requestLength: " + requestLength
                        + ", (requestOffset + requestLength): " + (requestOffset + requestLength) + ", allocSize: " + capacity;
    }
}
