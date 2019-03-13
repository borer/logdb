package org.borer.logdb.bit;

import sun.misc.Unsafe;

final class NativeMemoryAccess extends MemoryAccess
{
    private NativeMemoryAccess()
    {
    }

    public static long getLong(final long address)
    {
        return THE_UNSAFE.getLong(address);
    }

    public static void putLong(final long address, final long value)
    {
        THE_UNSAFE.putLong(address, value);
    }

    public static void putInt(final long address, final int value)
    {
        THE_UNSAFE.putInt(address, value);
    }

    public static int getInt(final long address)
    {
        return THE_UNSAFE.getInt(address);
    }

    public static void putBytes(final long destinationAddress, final byte[] sourceArray)
    {
        THE_UNSAFE.copyMemory(sourceArray, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, destinationAddress, sourceArray.length);
    }

    public static void getBytes(final long sourceAddress, final byte[] destinationArray)
    {
        //unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
        //unsafe.copyMemory(src, dst, size);
        //don't try to get the address of the msgBytes array or any other java object as it may be invalidated by GC
        //unsafe.setMemory(src, srcOffset, size, (byte)val); - set from address to address+size to val -- an easy fill
        THE_UNSAFE.copyMemory(null, sourceAddress, destinationArray, Unsafe.ARRAY_BYTE_BASE_OFFSET, destinationArray.length);
    }
}
