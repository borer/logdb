package org.borer.logdb.bit;

public class MemoryDirectImpl implements Memory
{
    private final long baseAddress;

    private long position;

    public MemoryDirectImpl(final long baseAddress)
    {
        this.baseAddress = baseAddress;
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
        putLong(position, value);
        position += Long.BYTES;
    }

    @Override
    public void putLong(final long offset, final long value)
    {
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
        return MemoryAccess.getLong(baseAddress + offset);
    }

    @Override
    public void putInt(final int value)
    {
        putInt(position, value);
        position += Integer.BYTES;
    }

    @Override
    public void putInt(final long offset, final int value)
    {
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
        MemoryAccess.getBytes(baseAddress + sourceOffset, destinationArray);
    }

    @Override
    public void putBytes(final byte[] sourceArray)
    {
        putBytes(position, sourceArray);
        position += sourceArray.length;
    }

    @Override
    public void putBytes(final long destinationOffset, final byte[] sourceArray)
    {
        MemoryAccess.putBytes(baseAddress + destinationOffset, sourceArray);
    }
}
