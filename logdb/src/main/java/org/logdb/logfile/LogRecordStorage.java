package org.logdb.logfile;

import org.logdb.bit.ChecksumUtil;
import org.logdb.bit.DirectMemory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

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
    private final @ByteSize long pageSize;

    LogRecordStorage(final Storage storage)
    {
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null");
        this.pageSize = storage.getPageSize();
        this.headerBuffer = ByteBuffer.allocate(LogRecordHeader.RECORD_HEADER_SIZE);
        headerBuffer.order(storage.getOrder());

        this.checksumer = new ChecksumUtil();
        this.directMemory = storage.getUninitiatedDirectMemoryPage();
        this.logRecordHeader = new LogRecordHeader();
    }

    @ByteOffset long writePut(
            final byte[] key,
            final byte[] value,
            final @Version long version,
            final @Milliseconds long timestamp) throws IOException
    {
        final int checksum = calculatePutChecksum(key, value, version, timestamp);
        logRecordHeader.initPut(
                checksum,
                StorageUnits.size(key.length),
                StorageUnits.size(value.length),
                version,
                timestamp);
        logRecordHeader.write(headerBuffer);

        final @ByteOffset long positionOffset = storage.append(headerBuffer);
        storage.append(ByteBuffer.wrap(key));
        storage.append(ByteBuffer.wrap(value));

        return positionOffset;
    }

    byte[] readRecordValue(final @ByteOffset long offset)
    {
        final PagePosition pagePosition = readHeader(offset);

        if (LogRecordType.UPDATE == logRecordHeader.getRecordType())
        {
            return readValue(pagePosition);
        }
        else if (LogRecordType.DELETE == logRecordHeader.getRecordType())
        {
            throw new IllegalArgumentException("offset " + offset + " refers to a delete record");
        }
        else
        {
            throw new IllegalArgumentException("offset " + offset + " refers to a invalid record ");
        }
    }

    @ByteOffset long writeDelete(
            final byte[] key,
            final @Version long version,
            final @Milliseconds long timestamp) throws IOException
    {
        final int checksum = calculateDeleteChecksum(key, version, timestamp);
        logRecordHeader.initDelete(
                checksum,
                StorageUnits.size(key.length),
                version,
                timestamp);
        logRecordHeader.write(headerBuffer);

        final @ByteOffset long offset = storage.append(headerBuffer);
        storage.append(ByteBuffer.wrap(key));

        return offset;
    }

    private int calculateDeleteChecksum(
            final byte[] key,
            final @Version long version,
            final @Milliseconds long timestamp)
    {
        checksumer.updateChecksum(key, ZERO_OFFSET, StorageUnits.size(key.length));
        checksumer.updateChecksum(version);
        checksumer.updateChecksum(timestamp);
        return checksumer.getAndResetChecksum();
    }

    private int calculatePutChecksum(
            final byte[] key,
            final byte[] value,
            final @Version long version,
            final @Milliseconds long timestamp)
    {
        checksumer.updateChecksum(key, ZERO_OFFSET, StorageUnits.size(key.length));
        checksumer.updateChecksum(value, ZERO_OFFSET, StorageUnits.size(value.length));
        checksumer.updateChecksum(version);
        checksumer.updateChecksum(timestamp);
        return checksumer.getAndResetChecksum();
    }

    private PagePosition readHeader(final @ByteOffset long offset)
    {
        @PageNumber long pageNumber = storage.getPageNumber(offset);
        @ByteOffset long offsetInsidePage = offset - storage.getOffset(pageNumber);

        //get page address
        storage.mapPage(pageNumber, directMemory);

        //try to read a complete header
        @ByteSize long pageLeftSpace = StorageUnits.size(pageSize - offsetInsidePage);
        final @ByteSize long recordHeaderBytesRead = StorageUnits.size(
                Math.min(pageLeftSpace, LogRecordHeader.RECORD_HEADER_SIZE));
        directMemory.getBytes(offsetInsidePage, recordHeaderBytesRead, headerBuffer.array());

        if (LogRecordHeader.RECORD_HEADER_SIZE == recordHeaderBytesRead)
        {
            offsetInsidePage += StorageUnits.offset(recordHeaderBytesRead);
        }
        else
        {
            //continue reading the header from the beginning of the next page
            pageNumber++;
            storage.mapPage(pageNumber, directMemory);

            final @ByteSize long remainingHeaderBytes = StorageUnits.size(
                    LogRecordHeader.RECORD_HEADER_SIZE - recordHeaderBytesRead);
            directMemory.getBytes(
                    ZERO_OFFSET,
                    remainingHeaderBytes,
                    headerBuffer.array(),
                    StorageUnits.offset(recordHeaderBytesRead));

            offsetInsidePage = StorageUnits.offset(remainingHeaderBytes);
        }

        try
        {
            logRecordHeader.read(headerBuffer);
        }
        catch (final RuntimeException e)
        {
            throw new IllegalStateException("Unable to read record header at offset " + offset, e);
        }

        return new PagePosition(pageNumber, offsetInsidePage);
    }

    private byte[] readValue(final PagePosition pagePosition)
    {
        @PageNumber long pageNumber = pagePosition.pageNumber;

        final ByteBuffer valueBuffer = ByteBuffer.allocate(logRecordHeader.getValueLength());
        final boolean isKeyValueInsidePage =
                (pagePosition.offsetInPage +
                        logRecordHeader.getKeyLength() +
                        logRecordHeader.getValueLength()) <= pageSize;

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
            @ByteSize long leftSize = logRecordHeader.getValueLength();
            while (leftSize > 0)
            {
                storage.mapPage(pageNumber, directMemory);
                final @ByteSize long bytesToRead = StorageUnits.size(Math.min(leftSize, pageSize));
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
