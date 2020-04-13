package org.logdb.storage.file.header;

import org.logdb.bit.MemoryOrder;
import org.logdb.checksum.ChecksumType;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.storage.file.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.Objects;

import static org.logdb.Config.LOG_DB_VERSION;
import static org.logdb.storage.StorageUnits.BYTE_SIZE;
import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;

public final class FileStorageStaticHeader
{
    private static final ByteOrder DEFAULT_HEADER_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private static final @ByteOffset long STATIC_HEADER_OFFSET = StorageUnits.ZERO_OFFSET;

    private static final byte[] LOG_DB_MAGIC_STRING = { 0x4c, 0x6f, 0x67, 0x44, 0x42, 0x00, 0x00}; // LogDb
    private static final @ByteOffset int BYTE_ORDER_OFFSET = StorageUnits.offset(LOG_DB_MAGIC_STRING.length); // size 7
    private static final @ByteSize int BYTE_ORDER_SIZE = BYTE_SIZE; // size 1

    private static final @ByteOffset int LOG_DB_VERSION_OFFSET = StorageUnits.offset(BYTE_ORDER_OFFSET + BYTE_ORDER_SIZE); // size 7 + 1 = 8
    private static final @ByteSize int LOG_DB_VERSION_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int PAGE_SIZE_OFFSET = StorageUnits.offset(LOG_DB_VERSION_OFFSET + LOG_DB_VERSION_SIZE);
    private static final @ByteSize int PAGE_SIZE_BYTES = INT_BYTES_SIZE;

    private static final @ByteOffset int PAGE_LOG_SIZE_OFFSET = StorageUnits.offset(PAGE_SIZE_OFFSET + PAGE_SIZE_BYTES);
    private static final @ByteSize int PAGE_LOG_SIZE_BYTES = INT_BYTES_SIZE;

    private static final @ByteOffset int SEGMENT_FILE_SIZE_OFFSET = StorageUnits.offset(PAGE_LOG_SIZE_OFFSET + PAGE_LOG_SIZE_BYTES);
    private static final @ByteSize int SEGMENT_FILE_SIZE_BYTES = LONG_BYTES_SIZE;

    private static final @ByteOffset int STATIC_CHECKSUM_TYPE_OFFSET = StorageUnits.offset(SEGMENT_FILE_SIZE_OFFSET + SEGMENT_FILE_SIZE_BYTES);
    private static final @ByteSize int STATIC_CHECKSUM_TYPE_OFFSET_SIZE = INT_BYTES_SIZE;

    private static final @ByteSize int STATIC_HEADER_SIZE = StorageUnits.size(LOG_DB_MAGIC_STRING.length) +
            BYTE_ORDER_SIZE +
            LOG_DB_VERSION_SIZE +
            PAGE_SIZE_BYTES +
            PAGE_LOG_SIZE_BYTES +
            SEGMENT_FILE_SIZE_BYTES +
            STATIC_CHECKSUM_TYPE_OFFSET_SIZE;

    private final ByteBuffer staticWriteBuffer;

    private final @ByteSize long segmentFileSize; //must be multiple of pageSize
    private final ByteOrder byteOrder;
    private final @ByteSize int pageSize; // Must be a power of two
    private final @ByteSize int pageLogSize; // Must be a power of two
    private final @Version int logDbVersion; //TODO: when loading a new file compare that we have compatible versions
    private final ChecksumType checksumType;

    private FileStorageStaticHeader(
            final ByteOrder byteOrder,
            final @ByteSize int pageSize,
            final @ByteSize int pageLogSize,
            final @ByteSize long segmentFileSize,
            final @Version int logDbVersion,
            final ChecksumType checksumType)
    {
        this.logDbVersion = logDbVersion;
        assert pageSize > 0 && ((pageSize & (pageSize - 1)) == 0) : "page size must be power of 2. Provided " + pageSize;
        assert segmentFileSize % pageSize == 0 : "segmentFileSize must be multiple of pageSize";
        assert pageSize > pageLogSize : "pageSize must be bigger than page log size";

        this.byteOrder = byteOrder;
        this.pageSize = pageSize;
        this.pageLogSize = pageLogSize;
        this.segmentFileSize = segmentFileSize;
        this.checksumType = Objects.requireNonNull(checksumType, "checksumType cannot be null");
        this.staticWriteBuffer = ByteBuffer.allocate(STATIC_HEADER_SIZE); // appendVersion, globalAppendOffset and currentFileAppendOffset
        this.staticWriteBuffer.order(DEFAULT_HEADER_BYTE_ORDER);
    }

    public static FileStorageStaticHeader newHeader(
            final ByteOrder byteOrder,
            final @ByteSize int pageSizeBytes,
            final @ByteSize int pageLogSize,
            final @ByteSize long segmentFileSize,
            final ChecksumType type)
    {
        return new FileStorageStaticHeader(
                byteOrder,
                pageSizeBytes,
                pageLogSize,
                segmentFileSize,
                LOG_DB_VERSION,
                type);
    }

    public static FileStorageStaticHeader readFrom(final SeekableByteChannel channel) throws IOException
    {
        channel.position(STATIC_HEADER_OFFSET);

        //Read static header
        final ByteBuffer staticHeaderBuffer = ByteBuffer.allocate(STATIC_HEADER_SIZE);
        FileUtils.readFully(channel, staticHeaderBuffer);
        staticHeaderBuffer.order(DEFAULT_HEADER_BYTE_ORDER);
        staticHeaderBuffer.rewind();

        final byte[] magicString = new byte[LOG_DB_MAGIC_STRING.length];
        staticHeaderBuffer.get(magicString);

        if (!Arrays.equals(LOG_DB_MAGIC_STRING, magicString))
        {
            throw new RuntimeException("DB file is not valid");
        }

        final ByteOrder byteOrder = getByteOrder(staticHeaderBuffer.get(BYTE_ORDER_OFFSET));
        final @Version int logDbVersion = StorageUnits.version(getIntegerInCorrectByteOrder(staticHeaderBuffer.getInt(LOG_DB_VERSION_OFFSET)));
        final @ByteSize int pageSize =
                StorageUnits.size(getIntegerInCorrectByteOrder(staticHeaderBuffer.getInt(PAGE_SIZE_OFFSET)));
        final @ByteSize int pageLogSize =
                StorageUnits.size(getIntegerInCorrectByteOrder(staticHeaderBuffer.getInt(PAGE_LOG_SIZE_OFFSET)));
        final @ByteSize long segmentFileSize =
                StorageUnits.size(getLongInCorrectByteOrder(staticHeaderBuffer.getLong(SEGMENT_FILE_SIZE_OFFSET)));

        final ChecksumType checksumType = ChecksumType.fromValue(staticHeaderBuffer.get(STATIC_CHECKSUM_TYPE_OFFSET));

        final @ByteSize long staticHeaderSize = getStaticHeaderSizeAlignedToNearestPage(pageSize);
        channel.position(staticHeaderSize);

        return new FileStorageStaticHeader(
                byteOrder,
                pageSize,
                pageLogSize,
                segmentFileSize,
                logDbVersion,
                checksumType);
    }

    public void writeAlign(final SeekableByteChannel destinationChannel) throws IOException
    {
        write(destinationChannel);
        final @ByteSize long staticHeaderSizeAlignedToNearestPage = getStaticHeaderSizeAlignedToNearestPage(pageSize);
        destinationChannel.position(staticHeaderSizeAlignedToNearestPage);
    }

    public void write(SeekableByteChannel destinationChannel) throws IOException
    {
        staticWriteBuffer.put(LOG_DB_MAGIC_STRING);
        staticWriteBuffer.put(BYTE_ORDER_OFFSET, getEncodedByteOrder(byteOrder));
        staticWriteBuffer.putInt(LOG_DB_VERSION_OFFSET, getIntegerInCorrectByteOrder(logDbVersion));
        staticWriteBuffer.putInt(PAGE_SIZE_OFFSET, getIntegerInCorrectByteOrder(pageSize));
        staticWriteBuffer.putInt(PAGE_LOG_SIZE_OFFSET, getIntegerInCorrectByteOrder(pageLogSize));
        staticWriteBuffer.putLong(SEGMENT_FILE_SIZE_OFFSET, getLongInCorrectByteOrder(segmentFileSize));
        staticWriteBuffer.putInt(STATIC_CHECKSUM_TYPE_OFFSET, getIntegerInCorrectByteOrder(checksumType.getTypeValue()));

        staticWriteBuffer.rewind();

        destinationChannel.position(STATIC_HEADER_OFFSET);

        FileUtils.writeFully(destinationChannel, staticWriteBuffer);
    }

    public @ByteSize long getSegmentFileSize()
    {
        return segmentFileSize;
    }

    public @ByteSize int getPageSize()
    {
        return pageSize;
    }

    public @ByteSize int getPageLogSize()
    {
        return pageLogSize;
    }

    public ByteOrder getOrder()
    {
        return byteOrder;
    }

    public @Version int getDbVersion()
    {
        return logDbVersion;
    }

    public ChecksumType getChecksumType()
    {
        return checksumType;
    }

    public static @ByteSize long getStaticHeaderSizeAlignedToNearestPage(final @ByteSize int pageSize)
    {
        final @PageNumber long pageNumbers = StorageUnits.pageNumber((STATIC_HEADER_SIZE / pageSize) + 1);
        return StorageUnits.size(pageNumbers * pageSize);
    }

    private static long getLongInCorrectByteOrder(final long value)
    {
        return MemoryOrder.isNativeOrder(DEFAULT_HEADER_BYTE_ORDER) ? value : Long.reverseBytes(value);
    }

    private static int getIntegerInCorrectByteOrder(final int value)
    {
        return MemoryOrder.isNativeOrder(DEFAULT_HEADER_BYTE_ORDER) ? value : Integer.reverseBytes(value);
    }

    private static byte getEncodedByteOrder(final ByteOrder byteOrder)
    {
        return (byte) ((ByteOrder.BIG_ENDIAN.equals(byteOrder)) ? 1 : 0);
    }

    private static ByteOrder getByteOrder(final byte byteOrderEncoded)
    {
        return byteOrderEncoded == 1 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString()
    {
        return "FileStorageStaticHeader{" +
                "staticWriteBuffer=" + staticWriteBuffer +
                ", segmentFileSize=" + segmentFileSize +
                ", byteOrder=" + byteOrder +
                ", pageSize=" + pageSize +
                ", pageLogSize=" + pageLogSize +
                ", logDbVersion=" + logDbVersion +
                ", checksumType=" + checksumType +
                '}';
    }
}
