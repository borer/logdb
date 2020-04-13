package org.logdb.storage.file.header;

import org.junit.jupiter.api.Test;
import org.logdb.checksum.ChecksumHelper;
import org.logdb.checksum.ChecksumType;
import org.logdb.checksum.Crc32;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileStorageDynamicHeaderTest
{
    private static final Crc32 CHECKSUM = new Crc32();
    private static final ChecksumHelper CHECKSUM_HELPER = new ChecksumHelper(CHECKSUM, ChecksumType.CRC32);

    @Test
    void shouldBeAbleToSaveAndLoadHeader() throws IOException
    {
        final @ByteSize int pageSizeBytes = StorageUnits.size(4096);
        final FileStorageDynamicHeader expectedHeader = FileStorageDynamicHeader.newHeader(pageSizeBytes, CHECKSUM_HELPER);

        final SeekableByteChannel channel = new ByteBufferSeekableByteChannel(
                ByteBuffer.allocate(pageSizeBytes * 3));
        expectedHeader.updateMeta(StorageUnits.ZERO_OFFSET, StorageUnits.ZERO_OFFSET, StorageUnits.INITIAL_VERSION);
        expectedHeader.writeAlign(channel);

        final FileStorageDynamicHeader actualHeader = FileStorageDynamicHeader.readFrom(channel, CHECKSUM_HELPER.getType(), pageSizeBytes);

        assertEquals(expectedHeader.getAppendVersion(), actualHeader.getAppendVersion());
        assertEquals(expectedHeader.getGlobalAppendOffset(), actualHeader.getGlobalAppendOffset());
        assertEquals(expectedHeader.getCurrentFileAppendOffset(), actualHeader.getCurrentFileAppendOffset());
        assertEquals(expectedHeader.getChecksumSize(), actualHeader.getChecksumSize());
    }
}