package org.logdb.storage.file.header;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

class ByteBufferSeekableByteChannel implements SeekableByteChannel
{
    private final ByteBuffer buffer;
    private int position;

    ByteBufferSeekableByteChannel(final ByteBuffer buffer)
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
        position = (int)newPosition;
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
