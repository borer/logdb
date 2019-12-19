package org.logdb.support;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class Assertions
{
    public static void assertExceptionWithMessage(final String expectedMessage, final Runnable runnable)
    {
        try
        {
            runnable.run();
            fail("should not execute");
        }
        catch (final Exception e)
        {
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    public static void assertArrayNotEquals(final byte[] expected, final byte[] actual)
    {
        try
        {
            assertArrayEquals(expected, actual);
        }
        catch (AssertionError e)
        {
            return;
        }
        fail("The arrays are equal");
    }
}
