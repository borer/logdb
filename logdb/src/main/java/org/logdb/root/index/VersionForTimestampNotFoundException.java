package org.logdb.root.index;

import org.logdb.time.Milliseconds;

public class VersionForTimestampNotFoundException extends RuntimeException
{
    public VersionForTimestampNotFoundException(final @Milliseconds long timestamp)
    {
        super("A version for timestamp " + timestamp + " was not found.");
    }
}
