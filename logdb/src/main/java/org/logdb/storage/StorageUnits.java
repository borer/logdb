package org.logdb.storage;

public class StorageUnits
{
    public static final @PageNumber long INVALID_PAGE_NUMBER = pageNumber(Long.MIN_VALUE);

    public static final @ByteOffset long INVALID_OFFSET = offset(Long.MIN_VALUE);

    public static final @Version long INITIAL_VERSION = StorageUnits.version(0);

    public static final @ByteOffset int ZERO_OFFSET = offset(0);
    public static final @ByteOffset int LONG_BYTES_OFFSET = offset(Long.BYTES);
    public static final @ByteOffset int INT_BYTES_OFFSET = offset(Integer.BYTES);
    public static final @ByteOffset int BYTE_OFFSET = offset(Byte.BYTES);

    public static final @ByteSize int ZERO_SIZE = size(0);
    public static final @ByteSize int LONG_BYTES_SIZE = size(Long.BYTES);
    public static final @ByteSize int INT_BYTES_SIZE = size(Integer.BYTES);
    public static final @ByteSize int BYTE_SIZE = size(Byte.BYTES);
    public static final @ByteSize int CHAR_SIZE = size(Character.BYTES);

    public static @Version long version(final long version)
    {
        return version;
    }

    public static @Version int version(final int version)
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

    public static @ByteSize long size(final long size)
    {
        return size;
    }

    public static @ByteSize int size(final int size)
    {
        return size;
    }
}
