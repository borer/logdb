package org.logdb.bit;

import java.nio.ByteOrder;

public class MemoryOrder
{
    static final ByteOrder nativeOrder = ByteOrder.nativeOrder();
    static final ByteOrder nonNativeOrder = (ByteOrder.LITTLE_ENDIAN.equals(nativeOrder))
            ? ByteOrder.BIG_ENDIAN
            : ByteOrder.LITTLE_ENDIAN;

    public static boolean isNativeOrder(final ByteOrder byteOrder)
    {
        if (byteOrder == null)
        {
            throw new IllegalArgumentException("ByteOrder parameter cannot be null.");
        }
        return (nativeOrder.equals(byteOrder));
    }
}
