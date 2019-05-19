package org.logdb.bit;

import java.nio.ByteOrder;

class MemoryOrder
{
    public static final ByteOrder nativeOrder = ByteOrder.nativeOrder();
    public static final ByteOrder nonNativeOrder = (nativeOrder == ByteOrder.LITTLE_ENDIAN)
            ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

    static boolean isNativeOrder(final ByteOrder byteOrder)
    {
        if (byteOrder == null)
        {
            throw new IllegalArgumentException("ByteOrder parameter cannot be null.");
        }
        return (nativeOrder == byteOrder);
    }
}
