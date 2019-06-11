package org.logdb.storage;

public class StorageUnits
{
    public static final @PageNumber long INVALID_PAGE_NUMBER = pageNumber(Long.MIN_VALUE);

    public static final @ByteOffset long INVALID_OFFSET = offset(-1L);

    public static final @ByteOffset int ZERO_OFFSET = offset(0);
    public static final @ByteOffset int LONG_BYTES_OFFSET = offset(Long.BYTES);
    public static final @ByteOffset int INT_BYTES_OFFSET = offset(Integer.BYTES);
    public static final @ByteOffset int BYTE_OFFSET = offset(Byte.BYTES);

    public static @Version long version(final long version)
    {
        return version;
    }

    public static @PageNumber long pageNumber(final long pageNumber)
    {
        return pageNumber;
    }

    public static @ByteOffset long offset(final long offset)
    {
        return offset;
    }

    public static @ByteOffset int offset(final int offset)
    {
        return offset;
    }
}
