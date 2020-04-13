package org.logdb.storage.file.header;

import org.junit.jupiter.api.Test;
import org.logdb.checksum.ChecksumHelper;
import org.logdb.checksum.ChecksumType;
import org.logdb.checksum.Crc32;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileStorageStaticHeaderTest
{
    private static final Crc32 CHECKSUM = new Crc32();
    private static final ChecksumHelper CHECKSUM_HELPER = new ChecksumHelper(CHECKSUM, ChecksumType.CRC32);

    @Test
    void shouldBeAbleToSaveAndLoadHeader() throws IOException
    {
        final @ByteSize int pageSizeBytes = StorageUnits.size(4096);
        final @ByteSize int pageLogSize = StorageUnits.size(1024);
        final FileStorageStaticHeader expectedHeader = FileStorageStaticHeader.newHeader(
                ByteOrder.BIG_ENDIAN,
                pageSizeBytes,
                pageLogSize,
                pageSizeBytes << 5,
                CHECKSUM_HELPER.getType());

        final SeekableByteChannel channel = new ByteBufferSeekableByteChannel(ByteBuffer.allocate(pageSizeBytes));
        expectedHeader.writeAlign(channel);

        final FileStorageStaticHeader actualHeader = FileStorageStaticHeader.readFrom(channel);

        assertEquals(expectedHeader.getPageSize(), actualHeader.getPageSize());
        assertEquals(expectedHeader.getSegmentFileSize(), actualHeader.getSegmentFileSize());
        assertEquals(expectedHeader.getOrder(), actualHeader.getOrder());
        assertEquals(expectedHeader.getDbVersion(), actualHeader.getDbVersion());
    }

}