package org.logdb.bbtree;

import org.logdb.storage.Version;

public class VersionNotFoundException extends RuntimeException
{
    public VersionNotFoundException(final @Version long version)
    {
        super("The version " + version + " was not found.");
    }
}
