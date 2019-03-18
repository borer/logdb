package org.borer.logdb.bit;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

abstract class MemoryAccess
{
    /**
     * Don't use {@link sun.misc.Unsafe#copyMemory} to copy blocks of memory larger than this
     * threshold, because internally it doesn't have safepoint polls, that may cause long
     * "Time To Safe Point" pauses in the application. This has been fixed in JDK 9 (see
     * https://bugs.openjdk.java.net/browse/JDK-8149596 and
     * https://bugs.openjdk.java.net/browse/JDK-8141491), but not in JDK 8, so the Memory library
     * should keep having this boilerplate as long as it supports Java 8.
     *
     * <p>A reference to this can be found in {@link java.nio.Bits}.</p>
     */
    static final int UNSAFE_COPY_THRESHOLD_BYTES = 1024 * 1024;

    static final Unsafe THE_UNSAFE;

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
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to load unsafe", e);
        }
    }

    static long getBaseAddressForDirectBuffer(final Buffer buffer)
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
}
