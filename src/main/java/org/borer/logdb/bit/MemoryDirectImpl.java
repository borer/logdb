package org.borer.logdb.bit;

public class MemoryDirectImpl implements Memory
{
    private final long baseAddress;
    private final long capacity;

    private long position;

    public MemoryDirectImpl(final long baseAddress, final long capacity)
    {
        this.baseAddress = baseAddress;
        this.capacity = capacity;
        this.position = 0;
    }

    @Override
    public void resetPosition()
    {
        position = 0;
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

        MemoryAccess.putLong(baseAddress + offset, value);
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

        return MemoryAccess.getLong(baseAddress + offset);
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

        MemoryAccess.putInt(baseAddress + offset, value);
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

        return MemoryAccess.getInt(baseAddress + offset);
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

        MemoryAccess.getBytes(baseAddress + sourceOffset, destinationArray);
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

        MemoryAccess.putBytes(baseAddress + destinationOffset, sourceArray);
    }

    private void assertBounds(final long requestOffset, final int requestLength)
    {
        assert ((requestOffset | requestLength | (requestOffset + requestLength) | (capacity - (requestOffset + requestLength))) >= 0) :
                "requestOffset: " + requestOffset + ", requestLength: " + requestLength
                 + ", (requestOffset + requestLength): " + (requestOffset + requestLength) + ", allocSize: " + capacity;
    }
}
