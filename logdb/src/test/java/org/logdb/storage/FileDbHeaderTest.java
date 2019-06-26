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
        final FileDbHeader expectedHeader = new FileDbHeader(
                ByteOrder.BIG_ENDIAN,
                123L,
                4096,
                4096 << 5,
                987654321L,
                123123L);

        final SeekableByteChannel channel = new ByteBufferSeekableByteChannel(ByteBuffer.allocate(FileDbHeader.HEADER_SIZE));
        expectedHeader.writeTo(channel);

        final FileDbHeader actualHeader = FileDbHeader.readFrom(channel);

        assertEquals(expectedHeader, actualHeader);
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
            dst.put(buffer);
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