package org.borer.logdb.bit;

public class Memory
{
    private final long baseAddress;

    private long position;

    public Memory(final long baseAddress)
    {
        this.baseAddress = baseAddress;
        this.position = 0;
    }

    public void resetPosition()
    {
        position = 0;
    }

    public void putLong(final long value)
    {
        putLong(position, value);
        position += Long.BYTES;
    }

    public void putLong(final long offset, final long value)
    {
        MemoryAccess.putLong(baseAddress + offset, value);
    }

    public long getLong()
    {
        return getLong(position);
    }

    public long getLong(final long offset)
    {
        return MemoryAccess.getLong(baseAddress + offset);
    }

    public void putInt(final int value)
    {
        putInt(position, value);
        position += Integer.BYTES;
    }

    public void putInt(final long offset, final int value)
    {
        MemoryAccess.putInt(baseAddress + offset, value);
    }

    public int getInt()
    {
        return getInt(position);
    }

    public int getInt(final long offset)
    {
        return MemoryAccess.getInt(baseAddress + offset);
    }

    public void getBytes(final byte[] array)
    {
        getBytes(position, array);
    }

    public void getBytes(final long sourceOffset, final byte[] destinationArray)
    {
        MemoryAccess.getBytes(baseAddress + sourceOffset, destinationArray);
    }

    public void putBytes(final byte[] sourceArray)
    {
        putBytes(position, sourceArray);
        position += sourceArray.length;
    }

    public void putBytes(final long destinationOffset, final byte[] sourceArray)
    {
        MemoryAccess.putBytes(baseAddress + destinationOffset, sourceArray);
    }
}
