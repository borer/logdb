package org.logdb.storage.file.header;

import org.logdb.bit.MemoryOrder;
import org.logdb.checksum.Checksum;
import org.logdb.checksum.ChecksumFactory;
import org.logdb.checksum.ChecksumHelper;
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

import static org.logdb.storage.StorageUnits.INITIAL_VERSION;
import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;
import static org.logdb.storage.file.header.FileStorageStaticHeader.getStaticHeaderSizeAlignedToNearestPage;

public final class FileStorageDynamicHeader
{
    private static final ByteOrder DEFAULT_HEADER_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private static final @ByteOffset int APPEND_VERSION_OFFSET = ZERO_OFFSET;
    private static final @ByteSize int APPEND_VERSION_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int GLOBAL_APPEND_OFFSET = StorageUnits.offset(APPEND_VERSION_OFFSET + APPEND_VERSION_SIZE);
    private static final @ByteSize int GLOBAL_APPEND_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int CURRENT_FILE_APPEND_OFFSET = StorageUnits.offset(GLOBAL_APPEND_OFFSET + GLOBAL_APPEND_SIZE);
    private static final @ByteSize int CURRENT_FILE_APPEND_OFFSET_SIZE = LONG_BYTES_SIZE;

    private static final @ByteOffset int DYNAMIC_CHECKSUM_LENGTH_OFFSET =
            StorageUnits.offset(CURRENT_FILE_APPEND_OFFSET + CURRENT_FILE_APPEND_OFFSET_SIZE);
    private static final @ByteSize int DYNAMIC_CHECKSUM_LENGTH_SIZE = INT_BYTES_SIZE;

    private static final @ByteSize int DYNAMIC_HEADER_SIZE_WITHOUT_CHECKSUM =
            APPEND_VERSION_SIZE + GLOBAL_APPEND_SIZE + CURRENT_FILE_APPEND_OFFSET_SIZE + DYNAMIC_CHECKSUM_LENGTH_SIZE;

    private final @ByteSize int pageSize; // Must be a power of two
    private final ByteBuffer dynamicWriteBuffer;

    private @Version long appendVersion;
    private @ByteOffset long globalAppendOffset;
    private @ByteOffset long currentFileAppendOffset;
    private byte[] checksumBuffer;
    private HeaderNumber headerNumber;
    private ChecksumHelper checksumHelper;

    private FileStorageDynamicHeader(
            final @Version long appendVersion,
            final @ByteSize int pageSize,
            final @ByteOffset long globalAppendOffset,
            final @ByteOffset long currentFileAppendOffset,
            final ChecksumHelper checksumHelper,
            final byte[] checksumBuffer,
            final HeaderNumber headerNumber)
    {
        this.headerNumber = Objects.requireNonNull(headerNumber, "header number cannot be null");
        assert pageSize > 0 && ((pageSize & (pageSize - 1)) == 0) : "page size must be power of 2. Provided " + pageSize;

        this.appendVersion = appendVersion;
        this.pageSize = pageSize;
        this.globalAppendOffset = globalAppendOffset;
        this.currentFileAppendOffset = currentFileAppendOffset;
        this.checksumBuffer = Objects.requireNonNull(checksumBuffer, "checksum buffer cannot be null");
        this.checksumHelper = Objects.requireNonNull(checksumHelper, "checksumHelper cannot be null");

        this.dynamicWriteBuffer = ByteBuffer.allocate(DYNAMIC_HEADER_SIZE_WITHOUT_CHECKSUM + checksumBuffer.length);
        this.dynamicWriteBuffer.order(DEFAULT_HEADER_BYTE_ORDER);
    }

    public static FileStorageDynamicHeader newHeader(final @ByteSize int pageSizeBytes, final ChecksumHelper checksumHelper)
    {
        final byte[] checksumBuffer = new byte[checksumHelper.getValueSize()];

        return new FileStorageDynamicHeader(
                INITIAL_VERSION,
                pageSizeBytes,
                StorageUnits.INVALID_OFFSET,
                StorageUnits.INVALID_OFFSET,
                checksumHelper,
                checksumBuffer,
                HeaderNumber.HEADER2);
    }

    public static FileStorageDynamicHeader readFrom(
            final SeekableByteChannel channel,
            final ChecksumType checksumType,
            final @ByteSize int pageSize) throws IOException
    {
        final Checksum checksum = ChecksumFactory.checksumFromType(checksumType);
        final ChecksumHelper checksumHelper = new ChecksumHelper(checksum, checksumType);

        final @ByteSize long staticHeaderSize = getStaticHeaderSizeAlignedToNearestPage(pageSize);
        final @ByteSize long dynamicHeaderSizeInPages = getDynamicHeaderSizeAlignedToNearestPage(pageSize);
        channel.position(staticHeaderSize);

        //Read first dynamic header
        final ByteBuffer dynamicBuffer1 = ByteBuffer.allocate(DYNAMIC_HEADER_SIZE_WITHOUT_CHECKSUM + checksumHelper.getValueSize());
        FileUtils.readFully(channel, dynamicBuffer1);
        dynamicBuffer1.order(DEFAULT_HEADER_BYTE_ORDER);
        dynamicBuffer1.rewind();

        channel.position(staticHeaderSize + dynamicHeaderSizeInPages);

        //Read second dynamic header
        final ByteBuffer dynamicBuffer2 = ByteBuffer.allocate(DYNAMIC_HEADER_SIZE_WITHOUT_CHECKSUM + checksumHelper.getValueSize());
        FileUtils.readFully(channel, dynamicBuffer2);
        dynamicBuffer2.order(DEFAULT_HEADER_BYTE_ORDER);
        dynamicBuffer2.rewind();

        final HeaderNumber headerNumber = chooseLatestValidDynamicHeader(dynamicBuffer1, dynamicBuffer2, checksumHelper);
        final ByteBuffer buffer = HeaderNumber.HEADER1 == headerNumber ? dynamicBuffer1 : dynamicBuffer2;

        final @Version long appendVersion = StorageUnits.version(getLongInCorrectByteOrder(buffer.getLong(APPEND_VERSION_OFFSET)));
        final @ByteOffset long lastPersistedOffset = StorageUnits.offset(getLongInCorrectByteOrder(buffer.getLong(GLOBAL_APPEND_OFFSET)));
        final @ByteOffset long appendOffset = StorageUnits.offset(getLongInCorrectByteOrder(buffer.getLong(CURRENT_FILE_APPEND_OFFSET)));

        final byte[] checksumBuffer = readDynamicChecksumBuffer(buffer);

        final FileStorageDynamicHeader fileStorageHeader = new FileStorageDynamicHeader(
                appendVersion,
                pageSize,
                lastPersistedOffset,
                appendOffset,
                checksumHelper,
                checksumBuffer,
                headerNumber);

        channel.position(staticHeaderSize + (dynamicHeaderSizeInPages * 2));

        return fileStorageHeader;
    }

    private static byte[] readDynamicChecksumBuffer(final ByteBuffer buffer)
    {
        final int checksumSize = buffer.getInt(DYNAMIC_CHECKSUM_LENGTH_OFFSET);
        final byte[] checksum = new byte[checksumSize];
        buffer.position(DYNAMIC_CHECKSUM_LENGTH_OFFSET + DYNAMIC_CHECKSUM_LENGTH_SIZE);
        buffer.get(checksum);
        buffer.rewind();
        return checksum;
    }

    private static HeaderNumber chooseLatestValidDynamicHeader(
            final ByteBuffer buffer1,
            final ByteBuffer buffer2,
            final ChecksumHelper checksumHelper)
    {
        final boolean isHeader1Valid = isDynamicHeaderValid(buffer1, checksumHelper);
        final boolean isHeader2Valid = isDynamicHeaderValid(buffer2, checksumHelper);

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

    private static boolean isDynamicHeaderValid(final ByteBuffer buffer, final ChecksumHelper checksumHelper)
    {
        final byte[] storedChecksum = readDynamicChecksumBuffer(buffer);

        if (storedChecksum.length != checksumHelper.getValueSize())
        {
            return false;
        }

        return checksumHelper.compareSingleChecksum(
                storedChecksum,
                buffer.array(),
                ZERO_OFFSET,
                DYNAMIC_HEADER_SIZE_WITHOUT_CHECKSUM);
    }

    public void writeAlign(final SeekableByteChannel destinationChannel) throws IOException
    {
        write(destinationChannel);
        final @ByteSize long staticHeaderSizeAlignedToNearestPage = getStaticHeaderSizeAlignedToNearestPage(pageSize);
        final @ByteSize long dynamicHeaderSizeAlignedToNearestPage = getDynamicHeaderSizeAlignedToNearestPage(pageSize);
        destinationChannel.position(staticHeaderSizeAlignedToNearestPage + (dynamicHeaderSizeAlignedToNearestPage * 2));
    }

    public void write(final SeekableByteChannel destinationChannel) throws IOException
    {
        dynamicWriteBuffer.putLong(APPEND_VERSION_OFFSET, getLongInCorrectByteOrder(appendVersion));
        dynamicWriteBuffer.putLong(GLOBAL_APPEND_OFFSET, getLongInCorrectByteOrder(globalAppendOffset));
        dynamicWriteBuffer.putLong(CURRENT_FILE_APPEND_OFFSET, getLongInCorrectByteOrder(currentFileAppendOffset));
        dynamicWriteBuffer.putInt(DYNAMIC_CHECKSUM_LENGTH_OFFSET, getIntegerInCorrectByteOrder(checksumBuffer.length));
        dynamicWriteBuffer.position(DYNAMIC_CHECKSUM_LENGTH_OFFSET + DYNAMIC_CHECKSUM_LENGTH_SIZE);
        dynamicWriteBuffer.put(checksumBuffer);
        dynamicWriteBuffer.rewind();

        final @ByteOffset long headerOffset = HeaderNumber.getOffset(headerNumber, pageSize);
        headerNumber = HeaderNumber.getNext(headerNumber);

        destinationChannel.position(headerOffset);

        FileUtils.writeFully(destinationChannel, dynamicWriteBuffer);
    }

    public @ByteOffset long getGlobalAppendOffset()
    {
        return globalAppendOffset;
    }

    public @ByteOffset long getCurrentFileAppendOffset()
    {
        return currentFileAppendOffset;
    }

    public @Version long getAppendVersion()
    {
        return appendVersion;
    }

    public void updateMeta(
            final @ByteOffset long globalAppendOffsetOffset,
            final @ByteOffset long currentFileAppendOffset,
            final @Version long appendVersion)
    {
        this.globalAppendOffset = globalAppendOffsetOffset;
        this.currentFileAppendOffset = currentFileAppendOffset;
        this.appendVersion = appendVersion;

        dynamicWriteBuffer.rewind();
        dynamicWriteBuffer.putLong(APPEND_VERSION_OFFSET, getLongInCorrectByteOrder(appendVersion));
        dynamicWriteBuffer.putLong(GLOBAL_APPEND_OFFSET, getLongInCorrectByteOrder(globalAppendOffsetOffset));
        dynamicWriteBuffer.putLong(CURRENT_FILE_APPEND_OFFSET, getLongInCorrectByteOrder(currentFileAppendOffset));

        this.checksumBuffer = checksumHelper.calculateSingleChecksum(dynamicWriteBuffer.array(), ZERO_OFFSET, DYNAMIC_HEADER_SIZE_WITHOUT_CHECKSUM);
        dynamicWriteBuffer.putInt(DYNAMIC_CHECKSUM_LENGTH_OFFSET, getIntegerInCorrectByteOrder(checksumBuffer.length));
        dynamicWriteBuffer.position(DYNAMIC_CHECKSUM_LENGTH_OFFSET + DYNAMIC_CHECKSUM_LENGTH_SIZE);
        dynamicWriteBuffer.put(checksumBuffer);
        dynamicWriteBuffer.rewind();
    }

    public static @ByteSize long getDynamicHeaderSizeAlignedToNearestPage(final @ByteSize int pageSize)
    {
        final @PageNumber long pageNumbers = StorageUnits.pageNumber((DYNAMIC_HEADER_SIZE_WITHOUT_CHECKSUM / pageSize) + 1);
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

    public @ByteSize int getChecksumSize()
    {
        return StorageUnits.size(checksumBuffer.length);
    }

    @Override
    public String toString()
    {
        return "FileStorageDynamicHeader{" +
                "pageSize=" + pageSize +
                ", dynamicWriteBuffer=" + dynamicWriteBuffer +
                ", appendVersion=" + appendVersion +
                ", globalAppendOffset=" + globalAppendOffset +
                ", currentFileAppendOffset=" + currentFileAppendOffset +
                ", checksumBuffer=" + Arrays.toString(checksumBuffer) +
                ", headerNumber=" + headerNumber +
                ", checksumHelper=" + checksumHelper +
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
