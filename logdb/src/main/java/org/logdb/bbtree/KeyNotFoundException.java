package org.logdb.bbtree;

public class KeyNotFoundException extends RuntimeException
{
    public KeyNotFoundException(final long key)
    {
        super("The key " + key + " was not found.");
    }
}
