package org.logdb.support;

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
}
