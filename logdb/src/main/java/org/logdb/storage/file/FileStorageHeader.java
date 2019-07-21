package org.logdb.storage.file;

import org.logdb.bit.ChecksumUtil;
import org.logdb.bit.MemoryOrder;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

import static org.logdb.Config.LOG_DB_VERSION;
import static org.logdb.storage.StorageUnits.BYTE_SIZE;
import static org.logdb.storage.StorageUnits.INITIAL_VERSION;
import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public final class FileStorageHeader implements FileHeader
{
    private static final int INVALID_CHECKSUM = 0;
    private static final ByteOrder DEFAULT_HEADER_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    /////Static Header
    static final @ByteOffset long STATIC_HEADER_OFFSET = StorageUnits.ZERO_OFFSET;

    private static final byte[] LOG_DB_MAGIC_STRING = { 0x4c, 0x6f, 0x67, 0x44, 0x42, 0x00, 0x00}; // LogDb
    private static final @ByteOffset int BYTE_ORDER_OFFSET = StorageUnits.offset(LOG_DB_MAGIC_STRING.length); // size 7
    private static final @ByteSize int BYTE_ORDER_SIZE = BYTE_SIZE; // size 1

    private static final @ByteOffset int LOG_DB_VERSION_OFFSET = StorageUnits.offset(BYTE_ORDER_OFFSET + BYTE_ORDER_SIZE); // size 7 + 1 = 8
    private static final @ByteSize int LOG_DB_VERSION_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int PAGE_SIZE_OFFSET = StorageUnits.offset(LOG_DB_VERSION_OFFSET + LOG_DB_VERSION_SIZE);
    private static final @ByteSize int PAGE_SIZE_BYTES = INT_BYTES_SIZE;

    private static final @ByteOffset int SEGMENT_FILE_SIZE_OFFSET = StorageUnits.offset(PAGE_SIZE_OFFSET + PAGE_SIZE_BYTES);
    private static final @ByteSize int SEGMENT_FILE_SIZE_BYTES = LONG_BYTES_SIZE;

    static final @ByteSize int STATIC_HEADER_SIZE = StorageUnits.size(LOG_DB_MAGIC_STRING.length) +
            BYTE_ORDER_SIZE +
            LOG_DB_VERSION_SIZE +
            PAGE_SIZE_BYTES +
            SEGMENT_FILE_SIZE_BYTES;

    /////End Static Header
    /////Dynamic Header

    private static final @ByteOffset int APPEND_VERSION_OFFSET = ZERO_OFFSET;
    private static final @ByteSize int APPEND_VERSION_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int GLOBAL_APPEND_OFFSET = StorageUnits.offset(APPEND_VERSION_OFFSET + APPEND_VERSION_SIZE);
    private static final @ByteSize int GLOBAL_APPEND_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int LAST_FILE_APPEND_OFFSET = StorageUnits.offset(GLOBAL_APPEND_OFFSET + GLOBAL_APPEND_SIZE);
    private static final @ByteSize int LAST_FILE_APPEND_OFFSET_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int CHECKSUM_OFFSET = StorageUnits.offset(LAST_FILE_APPEND_OFFSET + LAST_FILE_APPEND_OFFSET_SIZE);
    private static final @ByteSize int CHECKSUM_SIZE = INT_BYTES_SIZE;

    static final @ByteSize int DYNAMIC_HEADER_SIZE = APPEND_VERSION_SIZE + GLOBAL_APPEND_SIZE + LAST_FILE_APPEND_OFFSET_SIZE + CHECKSUM_SIZE;

    ////End Dynamic Header


    private final ByteBuffer staticWriteBuffer;
    private final ByteBuffer dynamicWriteBuffer;

    private @Version long appendVersion;
    private @ByteOffset long globalAppendOffset;
    private @ByteOffset long lastFileAppendOffset;
    private int checksum;
    private HeaderNumber headerNumber;

    private final @ByteSize long segmentFileSize; //must be multiple of pageSize
    private final ByteOrder byteOrder;
    private final @ByteSize int pageSize; // Must be a power of two
    private final @Version int logDbVersion; //TODO: when loading a new file compare that we have compatible versions

    private ChecksumUtil checksumUtil;

    private FileStorageHeader(
            final ByteOrder byteOrder,
            final @Version long appendVersion,
            final @ByteSize int pageSize,
            final @ByteSize long segmentFileSize,
            final @ByteOffset long globalAppendOffset,
            final @ByteOffset long lastFileAppendOffset,
            final @Version int logDbVersion,
            final int checksum,
            final HeaderNumber headerNumber)
    {
        this.logDbVersion = logDbVersion;
        this.headerNumber = headerNumber;
        assert pageSize > 0 && ((pageSize & (pageSize - 1)) == 0) : "page size must be power of 2. Provided " + pageSize;
        assert segmentFileSize % pageSize == 0 : "segmentFileSize must be multiple of pageSize";

        this.byteOrder = byteOrder;
        this.appendVersion = appendVersion;
        this.pageSize = pageSize;
        this.segmentFileSize = segmentFileSize;
        this.globalAppendOffset = globalAppendOffset;
        this.lastFileAppendOffset = lastFileAppendOffset;
        this.checksum = checksum;
        this.staticWriteBuffer = ByteBuffer.allocate(STATIC_HEADER_SIZE); // appendVersion, globalAppendOffset and lastFileAppendOffset
        this.staticWriteBuffer.order(DEFAULT_HEADER_BYTE_ORDER);

        this.dynamicWriteBuffer = ByteBuffer.allocate(DYNAMIC_HEADER_SIZE);
        this.dynamicWriteBuffer.order(DEFAULT_HEADER_BYTE_ORDER);

        this.checksumUtil = new ChecksumUtil();
    }

    static FileStorageHeader newHeader(
            final ByteOrder byteOrder,
            final @ByteSize int pageSizeBytes,
            final @ByteSize long segmentFileSize)
    {
        return new FileStorageHeader(
                byteOrder,
                INITIAL_VERSION,
                pageSizeBytes,
                segmentFileSize,
                StorageUnits.INVALID_OFFSET,
                StorageUnits.INVALID_OFFSET,
                LOG_DB_VERSION,
                INVALID_CHECKSUM,
                HeaderNumber.HEADER2);
    }

    public static FileStorageHeader readFrom(final SeekableByteChannel channel) throws IOException
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
        final @ByteSize long segmentFileSize =
                StorageUnits.size(getLongInCorrectByteOrder(staticHeaderBuffer.getLong(SEGMENT_FILE_SIZE_OFFSET)));

        final @ByteSize long staticHeaderSize = getStaticHeaderSizeAlignedToNearestPage(pageSize);
        final @ByteSize long dynamicHeaderSizeInPages = getDynamicHeaderSizeAlignedToNearestPage(pageSize);
        channel.position(staticHeaderSize);

        //Read first dynamic header
        final ByteBuffer dynamicBuffer1 = ByteBuffer.allocate(DYNAMIC_HEADER_SIZE);
        FileUtils.readFully(channel, dynamicBuffer1);
        dynamicBuffer1.order(DEFAULT_HEADER_BYTE_ORDER);
        dynamicBuffer1.rewind();

        channel.position(staticHeaderSize + dynamicHeaderSizeInPages);

        //Read second dynamic header
        final ByteBuffer dynamicBuffer2 = ByteBuffer.allocate(DYNAMIC_HEADER_SIZE);
        FileUtils.readFully(channel, dynamicBuffer2);
        dynamicBuffer2.order(DEFAULT_HEADER_BYTE_ORDER);
        dynamicBuffer2.rewind();

        final HeaderNumber headerNumber = chooseLatestValidDynamicHeader(dynamicBuffer1, dynamicBuffer2);
        final ByteBuffer buffer = HeaderNumber.HEADER1 == headerNumber ? dynamicBuffer1 : dynamicBuffer2;

        final @Version long appendVersion = StorageUnits.version(getLongInCorrectByteOrder(buffer.getLong(APPEND_VERSION_OFFSET)));
        final @ByteOffset long lastPersistedOffset = StorageUnits.offset(getLongInCorrectByteOrder(buffer.getLong(GLOBAL_APPEND_OFFSET)));
        final @ByteOffset long appendOffset = StorageUnits.offset(getLongInCorrectByteOrder(buffer.getLong(LAST_FILE_APPEND_OFFSET)));
        final int checksum = getIntegerInCorrectByteOrder(buffer.getInt(CHECKSUM_OFFSET));

        final FileStorageHeader fileStorageHeader = new FileStorageHeader(
                byteOrder,
                appendVersion,
                pageSize,
                segmentFileSize,
                lastPersistedOffset,
                appendOffset,
                logDbVersion,
                checksum,
                headerNumber);

        channel.position(staticHeaderSize + (dynamicHeaderSizeInPages * 2));

        return fileStorageHeader;
    }

    private static HeaderNumber chooseLatestValidDynamicHeader(
            final ByteBuffer buffer1,
            final ByteBuffer buffer2)
    {
        final boolean isHeader1Valid = isDynamicHeaderValid(buffer1);
        final boolean isHeader2Valid = isDynamicHeaderValid(buffer2);

        if (!isHeader1Valid && isHeader2Valid)
        {
            return HeaderNumber.HEADER2;
        }
        else if (isHeader1Valid && !isHeader2Valid)
        {
            return HeaderNumber.HEADER1;
        }
        else
        {
            final @Version long appendVersion1 = StorageUnits.version(getLongInCorrectByteOrder(buffer1.getLong(APPEND_VERSION_OFFSET)));
            final @Version long appendVersion2 = StorageUnits.version(getLongInCorrectByteOrder(buffer2.getLong(APPEND_VERSION_OFFSET)));

            if (appendVersion1 > appendVersion2)
            {
                return HeaderNumber.HEADER1;
            }
            else
            {
                return HeaderNumber.HEADER2;
            }
        }
    }

    private static boolean isDynamicHeaderValid(final ByteBuffer buffer)
    {
        final ChecksumUtil checksumUtil = new ChecksumUtil();
        final int storedChecksum = getIntegerInCorrectByteOrder(buffer.getInt(CHECKSUM_OFFSET));
        final int calculatedChecksum = checksumUtil.calculateSingleChecksum(buffer.array(), ZERO_OFFSET, DYNAMIC_HEADER_SIZE - CHECKSUM_SIZE);
        return storedChecksum == calculatedChecksum;
    }

    @Override
    public void writeHeadersAndAlign(final SeekableByteChannel channel) throws IOException
    {
        writeStaticHeaderTo(channel);
        writeDynamicHeaderTo(channel);
        final @ByteSize long staticHeaderSizeAlignedToNearestPage = getStaticHeaderSizeAlignedToNearestPage(pageSize);
        final @ByteSize long dynamicHeaderSizeAlignedToNearestPage = getDynamicHeaderSizeAlignedToNearestPage(pageSize);
        channel.position(staticHeaderSizeAlignedToNearestPage + (dynamicHeaderSizeAlignedToNearestPage * 2));
    }

    @Override
    public void writeStaticHeaderTo(SeekableByteChannel channel) throws IOException
    {
        staticWriteBuffer.put(LOG_DB_MAGIC_STRING);
        staticWriteBuffer.put(BYTE_ORDER_OFFSET, getEncodedByteOrder(byteOrder));
        staticWriteBuffer.putInt(LOG_DB_VERSION_OFFSET, getIntegerInCorrectByteOrder(logDbVersion));
        staticWriteBuffer.putInt(PAGE_SIZE_OFFSET, getIntegerInCorrectByteOrder(pageSize));
        staticWriteBuffer.putLong(SEGMENT_FILE_SIZE_OFFSET, getLongInCorrectByteOrder(segmentFileSize));

        staticWriteBuffer.rewind();

        channel.position(STATIC_HEADER_OFFSET);

        FileUtils.writeFully(channel, staticWriteBuffer);
    }

    @Override
    public void writeDynamicHeaderTo(final SeekableByteChannel channel) throws IOException
    {
        dynamicWriteBuffer.putLong(APPEND_VERSION_OFFSET, getLongInCorrectByteOrder(appendVersion));
        dynamicWriteBuffer.putLong(GLOBAL_APPEND_OFFSET, getLongInCorrectByteOrder(globalAppendOffset));
        dynamicWriteBuffer.putLong(LAST_FILE_APPEND_OFFSET, getLongInCorrectByteOrder(lastFileAppendOffset));
        dynamicWriteBuffer.putInt(CHECKSUM_OFFSET, getIntegerInCorrectByteOrder(checksum));

        dynamicWriteBuffer.rewind();

        final @ByteOffset long headerOffset = HeaderNumber.getOffset(headerNumber, pageSize);
        headerNumber = HeaderNumber.getNext(headerNumber);

        channel.position(headerOffset);

        FileUtils.writeFully(channel, dynamicWriteBuffer);
    }

    @Override
    public @ByteSize long getSegmentFileSize()
    {
        return segmentFileSize;
    }

    @Override
    public @ByteSize int getPageSize()
    {
        return pageSize;
    }

    @Override
    public ByteOrder getOrder()
    {
        return byteOrder;
    }

    @Override
    public @Version int getDbVersion()
    {
        return logDbVersion;
    }

    @Override
    public @ByteOffset long getGlobalAppendOffset()
    {
        return globalAppendOffset;
    }

    @Override
    public @ByteOffset long getLastFileAppendOffset()
    {
        return lastFileAppendOffset;
    }

    @Override
    public @Version long getAppendVersion()
    {
        return appendVersion;
    }

    @Override
    public void updateMeta(
            final @ByteOffset long lastPersistedOffset,
            final @ByteOffset long appendOffset,
            final @Version long appendVersion)
    {
        this.globalAppendOffset = lastPersistedOffset;
        this.lastFileAppendOffset = appendOffset;
        this.appendVersion = appendVersion;

        dynamicWriteBuffer.rewind();
        dynamicWriteBuffer.putLong(APPEND_VERSION_OFFSET, getLongInCorrectByteOrder(appendVersion));
        dynamicWriteBuffer.putLong(GLOBAL_APPEND_OFFSET, getLongInCorrectByteOrder(lastPersistedOffset));
        dynamicWriteBuffer.putLong(LAST_FILE_APPEND_OFFSET, getLongInCorrectByteOrder(appendOffset));

        final @ByteSize int length = StorageUnits.size(DYNAMIC_HEADER_SIZE - CHECKSUM_SIZE);
        this.checksum = checksumUtil.calculateSingleChecksum(dynamicWriteBuffer.array(), ZERO_OFFSET, length);
        dynamicWriteBuffer.putInt(CHECKSUM_OFFSET, getIntegerInCorrectByteOrder(checksum));
        dynamicWriteBuffer.rewind();
    }

    @Override
    public void flush(final boolean flushMeta)
    {
        //No-op
    }

    public static @ByteSize long getStaticHeaderSizeAlignedToNearestPage(final @ByteSize int pageSize)
    {
        final @PageNumber long pageNumbers = StorageUnits.pageNumber((STATIC_HEADER_SIZE / pageSize) + 1);
        return StorageUnits.size(pageNumbers * pageSize);
    }

    public static @ByteSize long getDynamicHeaderSizeAlignedToNearestPage(final @ByteSize int pageSize)
    {
        final @PageNumber long pageNumbers = StorageUnits.pageNumber((DYNAMIC_HEADER_SIZE / pageSize) + 1);
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
        return "FileStorageHeader{" +
                "staticWriteBuffer=" + staticWriteBuffer +
                ", dynamicWriteBuffer=" + dynamicWriteBuffer +
                ", appendVersion=" + appendVersion +
                ", globalAppendOffset=" + globalAppendOffset +
                ", lastFileAppendOffset=" + lastFileAppendOffset +
                ", checksum=" + checksum +
                ", headerNumber=" + headerNumber +
                ", segmentFileSize=" + segmentFileSize +
                ", byteOrder=" + byteOrder +
                ", pageSize=" + pageSize +
                ", logDbVersion=" + logDbVersion +
                ", checksumUtil=" + checksumUtil +
                '}';
    }

    private enum HeaderNumber
    {
        HEADER1,
        HEADER2;

        private static HeaderNumber getNext(final HeaderNumber current)
        {
            return HeaderNumber.HEADER1 == current ? HeaderNumber.HEADER2 : HeaderNumber.HEADER1;
        }

        private static @ByteOffset long getOffset(final HeaderNumber current, final @ByteSize int pageSize)
        {
            final @ByteOffset long dynamicHeaderOffset = HeaderNumber.HEADER1 == current
                    ? ZERO_OFFSET
                    : StorageUnits.offset(getDynamicHeaderSizeAlignedToNearestPage(pageSize));
            return StorageUnits.offset(getStaticHeaderSizeAlignedToNearestPage(pageSize) + dynamicHeaderOffset);
        }
    }
}
