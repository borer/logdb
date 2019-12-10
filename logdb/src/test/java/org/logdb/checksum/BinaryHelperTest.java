package org.logdb.checksum;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BinaryHelperTest
{
    private static final byte[] LONG_BYTES = new byte[]{-91, 101, -68, 0, 0, 0, 0, 0};
    private static final long LONG_VALUE = 12346789L;

    private static final byte[] INT_BYTES = new byte[]{85, -8, 6, 0};
    private static final int INT_VALUE = 456789;

    @Test
    void shouldConvertLongToBytes()
    {
        final byte[] bytes = new byte[8];
        BinaryHelper.longToBytes(LONG_VALUE, bytes);
        assertArrayEquals(LONG_BYTES, bytes);
    }

    @Test
    void shouldConvertBytesToLong()
    {
        final long value = BinaryHelper.bytesToLong(LONG_BYTES);
        assertEquals(LONG_VALUE, value);
    }

    @Test
    void shouldConvertIntegerToBytes()
    {
        final byte[] bytes = new byte[4];
        BinaryHelper.intToBytes(INT_VALUE, bytes);
        assertArrayEquals (INT_BYTES, bytes);
    }

    @Test
    void shouldConvertBytesToInt()
    {
        final long value = BinaryHelper.bytesToInt(INT_BYTES);
        assertEquals(INT_VALUE, value);
    }
}