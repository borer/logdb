package org.logdb.support;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Assertions
{
    public static void assertByteArrayEquals(final byte[] expected, final byte[] actual)
    {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++)
        {
            assertEquals(expected[i], actual[i]);
        }
    }
}
