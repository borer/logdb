package org.logdb.storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileDbHeaderTest
{
    @Test
    void shouldBeAbleToSaveAndLoadHeader() throws IOException
    {
        final @ByteSize int pageSizeBytes = StorageUnits.size(4096);
        final FileDbHeader expectedHeader = FileDbHeader.newHeader(
                ByteOrder.BIG_ENDIAN,
                pageSizeBytes,
                pageSizeBytes << 5);

        final SeekableByteChannel channel = new ByteBufferSeekableByteChannel(ByteBuffer.allocate(pageSizeBytes));
        expectedHeader.writeTo(channel);

        final FileDbHeader actualHeader = FileDbHeader.readFrom(channel);

        assertEquals(expectedHeader.pageSize, actualHeader.pageSize);
        assertEquals(expectedHeader.segmentFileSize, actualHeader.segmentFileSize);
        assertEquals(expectedHeader.byteOrder, actualHeader.byteOrder);
        assertEquals(expectedHeader.logDbVersion, actualHeader.logDbVersion);
        assertEquals(expectedHeader.getAppendVersion(), actualHeader.getAppendVersion());
        assertEquals(expectedHeader.getGlobalAppendOffset(), actualHeader.getGlobalAppendOffset());
        assertEquals(expectedHeader.getLastFileAppendOffset(), actualHeader.getLastFileAppendOffset());
    }

    private static class ByteBufferSeekableByteChannel implements SeekableByteChannel
    {
        private final ByteBuffer buffer;

        private ByteBufferSeekableByteChannel(final ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public int read(final ByteBuffer dst)
        {
            buffer.rewind();
            dst.put(buffer.array(), 0, FileDbHeader.HEADER_SIZE);
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