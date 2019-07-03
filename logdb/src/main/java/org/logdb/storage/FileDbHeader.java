package org.logdb.storage;

import org.logdb.bit.MemoryOrder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

import static org.logdb.Config.INITIAL_STORAGE_VERSION;
import static org.logdb.Config.LOG_DB_VERSION;
import static org.logdb.storage.StorageUnits.BYTE_SIZE;
import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;

public final class FileDbHeader
{
    private static final ByteOrder DEFAULT_HEADER_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private static final byte[] LOG_DB_MAGIC_STRING = { 0x4c, 0x6f, 0x67, 0x44, 0x42, 0x00, 0x00}; // LogDb
    private static final @ByteOffset int BYTE_ORDER_OFFSET = StorageUnits.offset(LOG_DB_MAGIC_STRING.length); // size 7
    private static final @ByteSize int BYTE_ORDER_SIZE = BYTE_SIZE; // size 1

    private static final @ByteOffset int LOG_DB_VERSION_OFFSET = StorageUnits.offset(BYTE_ORDER_OFFSET + BYTE_ORDER_SIZE); // size 7 + 1 = 8
    private static final @ByteSize int LOG_DB_VERSION_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int PAGE_SIZE_OFFSET = StorageUnits.offset(LOG_DB_VERSION_OFFSET + LOG_DB_VERSION_SIZE);
    private static final @ByteSize int PAGE_SIZE_BYTES = INT_BYTES_SIZE;

    private static final @ByteOffset int SEGMENT_FILE_SIZE_OFFSET = StorageUnits.offset(PAGE_SIZE_OFFSET + PAGE_SIZE_BYTES);
    private static final @ByteSize int SEGMENT_FILE_SIZE_BYTES = LONG_BYTES_SIZE;

    private static final @ByteOffset int APPEND_VERSION_OFFSET = StorageUnits.offset(SEGMENT_FILE_SIZE_OFFSET + SEGMENT_FILE_SIZE_BYTES);
    private static final @ByteSize int APPEND_VERSION_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int GLOBAL_APPEND_OFFSET = StorageUnits.offset(APPEND_VERSION_OFFSET + APPEND_VERSION_SIZE);
    private static final @ByteSize int GLOBAL_APPEND_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int LAST_FILE_APPEND_OFFSET = StorageUnits.offset(GLOBAL_APPEND_OFFSET + GLOBAL_APPEND_SIZE);
    private static final @ByteSize int LAST_FILE_APPEND_OFFSET_SIZE = LONG_BYTES_SIZE;

    static final @ByteOffset long HEADER_OFFSET = StorageUnits.ZERO_OFFSET;
    static final @ByteSize int HEADER_SIZE = StorageUnits.size(LOG_DB_MAGIC_STRING.length) +
            BYTE_ORDER_SIZE +
            LOG_DB_VERSION_SIZE +
            PAGE_SIZE_BYTES +
            SEGMENT_FILE_SIZE_BYTES +
            APPEND_VERSION_SIZE +
            GLOBAL_APPEND_SIZE +
            LAST_FILE_APPEND_OFFSET_SIZE;

    private final ByteBuffer writeBuffer;

    private @Version long appendVersion;
    private @ByteOffset long globalAppendOffset;
    private @ByteOffset long lastFileAppendOffset;

    final @ByteSize long segmentFileSize; //must be multiple of pageSize

    public final ByteOrder byteOrder;
    public final @ByteSize int pageSize; // Must be a power of two
    public final @Version int logDbVersion; //TODO: when loading a new file compare that we have compatible versions

    private FileDbHeader(
            final ByteOrder byteOrder,
            final @Version long appendVersion,
            final @ByteSize int pageSize,
            final @ByteSize long segmentFileSize,
            final @ByteOffset long globalAppendOffset,
            final @ByteOffset long lastFileAppendOffset,
            final @Version int logDbVersion)
    {
        this.logDbVersion = logDbVersion;
        assert pageSize > 0 && ((pageSize & (pageSize - 1)) == 0) : "page size must be power of 2. Provided " + pageSize;
        assert segmentFileSize % pageSize == 0 : "segmentFileSize must be multiple of pageSize";

        this.byteOrder = byteOrder;
        this.appendVersion = appendVersion;
        this.pageSize = pageSize;
        this.segmentFileSize = segmentFileSize;
        this.globalAppendOffset = globalAppendOffset;
        this.lastFileAppendOffset = lastFileAppendOffset;
        this.writeBuffer = ByteBuffer.allocate(HEADER_SIZE); // appendVersion, globalAppendOffset and lastFileAppendOffset
        this.writeBuffer.order(DEFAULT_HEADER_BYTE_ORDER);
    }

    static FileDbHeader newHeader(
            final ByteOrder byteOrder,
            final @ByteSize int pageSizeBytes,
            final @ByteSize long segmentFileSize)
    {
        return new FileDbHeader(
                byteOrder,
                INITIAL_STORAGE_VERSION,
                pageSizeBytes,
                segmentFileSize,
                StorageUnits.INVALID_OFFSET,
                StorageUnits.INVALID_OFFSET,
                LOG_DB_VERSION);
    }

    public static FileDbHeader readFrom(final SeekableByteChannel channel) throws IOException
    {
        final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        FileUtils.readFully(channel, buffer);
        buffer.order(DEFAULT_HEADER_BYTE_ORDER);
        buffer.rewind();

        final byte[] magicString = new byte[LOG_DB_MAGIC_STRING.length];
        buffer.get(magicString);

        if (!Arrays.equals(LOG_DB_MAGIC_STRING, magicString))
        {
            throw new RuntimeException("DB file is not valid");
        }

        final ByteOrder byteOrder = getByteOrder(buffer.get(BYTE_ORDER_OFFSET));
        final @Version int logDbVersion = StorageUnits.version(getIntegerInCorrectByteOrder(buffer.getInt(LOG_DB_VERSION_OFFSET)));
        final @ByteSize int pageSize =
                StorageUnits.size(getIntegerInCorrectByteOrder(buffer.getInt(PAGE_SIZE_OFFSET)));
        final @ByteSize long segmentFileSize =
                StorageUnits.size(getLongInCorrectByteOrder(buffer.getLong(SEGMENT_FILE_SIZE_OFFSET)));
        final @Version long appendVersion = StorageUnits.version(getLongInCorrectByteOrder(buffer.getLong(APPEND_VERSION_OFFSET)));
        final @ByteOffset long lastPersistedOffset = StorageUnits.offset(getLongInCorrectByteOrder(buffer.getLong(GLOBAL_APPEND_OFFSET)));
        final @ByteOffset long appendOffset = StorageUnits.offset(getLongInCorrectByteOrder(buffer.getLong(LAST_FILE_APPEND_OFFSET)));

        final FileDbHeader fileDbHeader = new FileDbHeader(
                byteOrder,
                appendVersion,
                pageSize,
                segmentFileSize,
                lastPersistedOffset,
                appendOffset,
                logDbVersion);

        channel.position(fileDbHeader.getHeaderSizeAlignedToNearestPage());

        return fileDbHeader;
    }

    void writeToAndPageAlign(final SeekableByteChannel channel) throws IOException
    {
        writeTo(channel);
        channel.position(getHeaderSizeAlignedToNearestPage());
    }

    void writeTo(final SeekableByteChannel channel) throws IOException
    {
        writeBuffer.put(LOG_DB_MAGIC_STRING);
        writeBuffer.put(BYTE_ORDER_OFFSET, getEncodedByteOrder(byteOrder));
        writeBuffer.putInt(LOG_DB_VERSION_OFFSET, getIntegerInCorrectByteOrder(logDbVersion));
        writeBuffer.putInt(PAGE_SIZE_OFFSET, getIntegerInCorrectByteOrder(pageSize));
        writeBuffer.putLong(SEGMENT_FILE_SIZE_OFFSET, getLongInCorrectByteOrder(segmentFileSize));
        writeBuffer.putLong(APPEND_VERSION_OFFSET, getLongInCorrectByteOrder(appendVersion));
        writeBuffer.putLong(GLOBAL_APPEND_OFFSET, getLongInCorrectByteOrder(globalAppendOffset));
        writeBuffer.putLong(LAST_FILE_APPEND_OFFSET, getLongInCorrectByteOrder(lastFileAppendOffset));

        writeBuffer.rewind();

        FileUtils.writeFully(channel, writeBuffer);
    }

    public @ByteSize long getHeaderSizeAlignedToNearestPage()
    {
        return StorageUnits.size(getHeaderSizeInPages() * pageSize);
    }

    public @ByteOffset long getGlobalAppendOffset()
    {
        return globalAppendOffset;
    }

    public @ByteOffset long getLastFileAppendOffset()
    {
        return lastFileAppendOffset;
    }

    @Version long getAppendVersion()
    {
        return appendVersion;
    }

    void updateMeta(
            final @ByteOffset long lastPersistedOffset,
            final @ByteOffset long appendOffset,
            final @Version long appendVersion)
    {
        this.globalAppendOffset = lastPersistedOffset;
        this.lastFileAppendOffset = appendOffset;
        this.appendVersion = appendVersion;

        writeBuffer.rewind();
        writeBuffer.putLong(APPEND_VERSION_OFFSET, getLongInCorrectByteOrder(appendVersion));
        writeBuffer.putLong(GLOBAL_APPEND_OFFSET, getLongInCorrectByteOrder(lastPersistedOffset));
        writeBuffer.putLong(LAST_FILE_APPEND_OFFSET, getLongInCorrectByteOrder(appendOffset));
        writeBuffer.rewind();
    }

    private @ByteSize int getHeaderSizeInPages()
    {
        return StorageUnits.size((HEADER_SIZE / pageSize) + 1);
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
        return "FileDbHeader{" +
                "writeBuffer=" + writeBuffer +
                ", appendVersion=" + appendVersion +
                ", globalAppendOffset=" + globalAppendOffset +
                ", lastFileAppendOffset=" + lastFileAppendOffset +
                ", segmentFileSize=" + segmentFileSize +
                ", byteOrder=" + byteOrder +
                ", pageSize=" + pageSize +
                ", logDbVersion=" + logDbVersion +
                '}';
    }
}
