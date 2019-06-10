package org.logdb.storage;

public class StorageUnits
{
    public static final @PageNumber long INVALID_PAGE_NUMBER = pageNumber(Long.MIN_VALUE);

    public static @Version long version(final long version)
    {
        return version;
    }

    public static @PageNumber long pageNumber(final long pageNumber)
    {
        return pageNumber;
    }
}
