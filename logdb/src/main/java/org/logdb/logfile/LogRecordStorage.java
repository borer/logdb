package org.logdb.logfile;

import org.logdb.bit.ChecksumUtil;
import org.logdb.bit.DirectMemory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.nio.ByteBuffer;

import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

/**
 * The structure of the LogRecordStorage on disc is
 * ------------------------------------------ 0
 * |                 Header                 |
 * ------------------------------------------ h
 * |                 Key                    |
 * ------------------------------------------ h + keyLength
 * |                 Value                  |
 * ------------------------------------------ h + keyLength + valueLength (N)
 * |                 ....                   |
 * ------------------------------------------ N + 1
 */
class LogRecordStorage
{
    private final ChecksumUtil checksumer;
    private final ByteBuffer headerBuffer;
    private final Storage storage;
    private final DirectMemory directMemory;
    private final LogRecordHeader logRecordHeader;

    LogRecordStorage(final Storage storage)
    {
        this.storage = storage;
        this.headerBuffer = ByteBuffer.allocate(LogRecordHeader.RECORD_HEADER_SIZE);
        this.checksumer = new ChecksumUtil();
        this.directMemory = storage.getUninitiatedDirectMemoryPage();
        this.logRecordHeader = new LogRecordHeader();
    }

    long write(final byte[] key, final byte[] value, final @Version long version, final @Milliseconds long timestamp)
    {
        final int checksum = calculateChecksum(key, value, version, timestamp);
        logRecordHeader.init(checksum, key.length, value.length, version, timestamp);
        logRecordHeader.write(headerBuffer);

        final long positionOffset = storage.write(headerBuffer);
        storage.write(ByteBuffer.wrap(key));
        storage.write(ByteBuffer.wrap(value));

        return positionOffset;
    }

    private int calculateChecksum(
            final byte[] key,
            final byte[] value,
            final @Version long version,
            final @Milliseconds long timestamp)
    {
        checksumer.updateChecksum(key, 0, key.length);
        checksumer.updateChecksum(value, 0, value.length);
        checksumer.updateChecksum(version);
        checksumer.updateChecksum(timestamp);
        return checksumer.getAndResetChecksum();
    }

    byte[] readRecordValue(final @ByteOffset long offset)
    {
        final PagePosition pagePosition = readHeader(offset);
        return readValue(pagePosition);
    }

    private PagePosition readHeader(final @ByteOffset long offset)
    {
        @PageNumber long pageNumber = storage.getPageNumber(offset);
        @ByteOffset long offsetInsidePage = offset - storage.getOffset(pageNumber);

        //get page address
        final @ByteOffset long baseOffsetForPageNumber = storage.getBaseOffsetForPageNumber(pageNumber);
        directMemory.setBaseAddress(baseOffsetForPageNumber);

        //try to read a complete header
        long pageLeftSpace = storage.getPageSize() - offsetInsidePage;
        final long recordHeaderBytesRead = Math.min(pageLeftSpace, LogRecordHeader.RECORD_HEADER_SIZE);
        directMemory.getBytes(offsetInsidePage, recordHeaderBytesRead, headerBuffer.array());

        if (LogRecordHeader.RECORD_HEADER_SIZE == recordHeaderBytesRead)
        {
            offsetInsidePage += StorageUnits.offset(recordHeaderBytesRead);
        }
        else
        {
            //continue reading the header from the beginning of the next page
            pageNumber++;
            final @ByteOffset long baseOffsetForNextPage = storage.getBaseOffsetForPageNumber(pageNumber);
            directMemory.setBaseAddress(baseOffsetForNextPage);

            final @ByteOffset long remainingHeaderBytes = StorageUnits.offset(LogRecordHeader.RECORD_HEADER_SIZE - recordHeaderBytesRead);
            directMemory.getBytes(ZERO_OFFSET, remainingHeaderBytes, headerBuffer.array(), StorageUnits.offset(recordHeaderBytesRead));

            offsetInsidePage = StorageUnits.offset(remainingHeaderBytes);
        }

        logRecordHeader.read(headerBuffer);

        return new PagePosition(pageNumber, offsetInsidePage);
    }

    private byte[] readValue(final PagePosition pagePosition)
    {
        @PageNumber long pageNumber = pagePosition.pageNumber;

        final ByteBuffer valueBuffer = ByteBuffer.allocate(logRecordHeader.getValueLength());
        final boolean isKeyValueInsidePage =
                pagePosition.offsetInPage +
                        logRecordHeader.getKeyLength() +
                        logRecordHeader.getValueLength() <= storage.getPageSize();

        if (isKeyValueInsidePage)
        {
            final @ByteOffset long valueOffset =
                    StorageUnits.offset(pagePosition.offsetInPage + logRecordHeader.getKeyLength());
            directMemory.getBytes(valueOffset, logRecordHeader.getValueLength(), valueBuffer.array());
        }
        else
        {
            final @PageNumber long keyLengthInPages = storage.getPageNumber(StorageUnits.offset(logRecordHeader.getKeyLength()));
            @ByteOffset long offsetPage =
                    StorageUnits.offset(pagePosition.offsetInPage + (logRecordHeader.getKeyLength() - storage.getOffset(keyLengthInPages)));

            //skip the key bytes
            pageNumber += keyLengthInPages;

            @ByteOffset long totalBytesRead = ZERO_OFFSET;
            long leftSize = logRecordHeader.getValueLength();
            while (leftSize > 0)
            {
                final @ByteOffset long baseOffsetForPageNumber = storage.getBaseOffsetForPageNumber(pageNumber);
                directMemory.setBaseAddress(baseOffsetForPageNumber);
                final long bytesToRead = Math.min(leftSize, storage.getPageSize());
                directMemory.getBytes(offsetPage, bytesToRead, valueBuffer.array(), totalBytesRead);
                leftSize -= bytesToRead;
                totalBytesRead += StorageUnits.offset(bytesToRead);
                pageNumber++;
            }
        }

        return valueBuffer.array();
    }

    private static final class PagePosition
    {
        final @PageNumber long pageNumber;
        final @ByteOffset long offsetInPage;

        PagePosition(final @PageNumber long pageNumber, final @ByteOffset long offsetInPage)
        {
            this.pageNumber = pageNumber;
            this.offsetInPage = offsetInPage;
        }
    }
}
