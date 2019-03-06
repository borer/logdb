package org.borer.logdb.bit;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

public final class MemoryAccess
{
    private static final Unsafe THE_UNSAFE;
    private static final boolean SHOULD_REVERSE;

    static
    {
        try
        {
            final PrivilegedExceptionAction<Unsafe> action = () ->
            {
                Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                return (Unsafe) theUnsafe.get(null);
            };

            THE_UNSAFE = AccessController.doPrivileged(action);
            SHOULD_REVERSE = shouldReverseBytes();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to load unsafe", e);
        }
    }

    private MemoryAccess()
    {
    }

    public static long getBaseAddressForDirectBuffer(final Buffer buffer)
    {
        if (!buffer.isDirect())
        {
            throw new RuntimeException("Buffer has to be native allocated or mapped.");
        }

        try
        {
            final PrivilegedExceptionAction<Long> action = () ->
            {
                final Field addressField = Buffer.class.getDeclaredField("address");
                addressField.setAccessible(true);
                return (Long)addressField.get(buffer);
            };

            return AccessController.doPrivileged(action);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to load buffer address", e);
        }
    }

    public static long getLongFromArray(final long baseAddress, final int index)
    {
        return THE_UNSAFE.getLong(baseAddress + (index * Long.BYTES));
    }

    public static void putLongFromArray(final long baseAddress, final int index, final long value)
    {
        THE_UNSAFE.putLong(baseAddress + (index * Long.BYTES), value);
    }

    public static long getLong(final long address)
    {
        final long anLong = THE_UNSAFE.getLong(address);
        return SHOULD_REVERSE ? Long.reverseBytes(anLong) : anLong;
    }

    public static void putLong(final long address, final long value)
    {
        final long anLong = SHOULD_REVERSE ? Long.reverseBytes(value) : value;
        THE_UNSAFE.putLong(address, anLong);
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
        //unsafe.copyMemory(src, srcOffser, dst, dstOffset, size);
        //unsafe.copyMemory(src, dst, size);
        //don't try to get the address of the msgBytes array or any other java object as it may be invalidated by GC
        //unsafe.setMemory(src, srcOffser, size, (byte)val); - set from address to address+size to val -- an easy fill
        THE_UNSAFE.copyMemory(null, sourceAddress, destinationArray, Unsafe.ARRAY_BYTE_BASE_OFFSET, destinationArray.length);
    }

    private static void writeLongBigEndian(final byte[] buffer, final int offset, final long l)
    {
        buffer[offset] = (byte) (l >>> 56);
        buffer[offset + 1] = (byte) (l >>> 48);
        buffer[offset + 2] = (byte) (l >>> 40);
        buffer[offset + 3] = (byte) (l >>> 32);
        buffer[offset + 4] = (byte) (l >>> 24);
        buffer[offset + 5] = (byte) (l >>> 16);
        buffer[offset + 6] = (byte) (l >>> 8);
        buffer[offset + 7] = (byte) (l);
    }

    private static boolean shouldReverseBytes()
    {
        final byte[] b = new byte[8];
        final long expected = 98237454L;
        writeLongBigEndian(b, 0, expected);
        final long actual = THE_UNSAFE.getLong(b, Unsafe.ARRAY_BYTE_BASE_OFFSET);
        return actual != expected;
    }
}
