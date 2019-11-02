package org.logdb.storage.file;

import org.junit.jupiter.api.Test;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileStorageHeaderTest
{
    @Test
    void shouldBeAbleToSaveAndLoadHeader() throws IOException
    {
        final @ByteSize int pageSizeBytes = StorageUnits.size(4096);
        final FileStorageHeader expectedHeader = FileStorageHeader.newHeader(
                ByteOrder.BIG_ENDIAN,
                pageSizeBytes,
                pageSizeBytes << 5);

        final SeekableByteChannel channel = new ByteBufferSeekableByteChannel(
                ByteBuffer.allocate(pageSizeBytes * 3));
        expectedHeader.updateMeta(StorageUnits.ZERO_OFFSET, StorageUnits.ZERO_OFFSET, StorageUnits.INITIAL_VERSION);
        expectedHeader.writeHeadersAndAlign(channel);

        final FileStorageHeader actualHeader = FileStorageHeader.readFrom(channel);

        assertEquals(expectedHeader.getPageSize(), actualHeader.getPageSize());
        assertEquals(expectedHeader.getSegmentFileSize(), actualHeader.getSegmentFileSize());
        assertEquals(expectedHeader.getOrder(), actualHeader.getOrder());
        assertEquals(expectedHeader.getDbVersion(), actualHeader.getDbVersion());
        assertEquals(expectedHeader.getAppendVersion(), actualHeader.getAppendVersion());
        assertEquals(expectedHeader.getGlobalAppendOffset(), actualHeader.getGlobalAppendOffset());
        assertEquals(expectedHeader.getCurrentFileAppendOffset(), actualHeader.getCurrentFileAppendOffset());
    }

    private static class ByteBufferSeekableByteChannel implements SeekableByteChannel
    {
        private final ByteBuffer buffer;
        private int position;

        private ByteBufferSeekableByteChannel(final ByteBuffer buffer)
        {
            this.buffer = buffer;
            this.position = 0;
        }

        @Override
        public int read(final ByteBuffer dst)
        {
            buffer.rewind();
            dst.put(buffer.array(), position, dst.capacity());
            position += dst.capacity();
            return dst.limit();
        }

        @Override
        public int write(final ByteBuffer src)
        {
            buffer.put(src);
            buffer.rewind();
            return src.limit();
        }

        @Override
        public long position()
        {
            return buffer.position();
        }

        @Override
        public SeekableByteChannel position(final long newPosition)
        {
            buffer.position((int)newPosition);
            return this;
        }

        @Override
        public long size()
        {
            return buffer.capacity();
        }

        @Override
        public SeekableByteChannel truncate(final long size)
        {
            return null;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public void close()
        {

        }
    }
}