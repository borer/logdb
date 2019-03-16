package org.borer.logdb.bit;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

abstract class MemoryAccess
{
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
