package org.logdb.support;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

final class FileReaderStorage implements Storage
{
    private final FileChannel fileChannel;
    private final @ByteSize int pageSize;
    private final ByteOrder fileByteOrder;
    private MappedByteBuffer mappedByteBuffer;

    FileReaderStorage(final FileChannel fileChannel, final @ByteSize int pageSize, final ByteOrder fileByteOrder)
    {
        this.fileChannel = fileChannel;
        this.pageSize = pageSize;
        this.fileByteOrder = fileByteOrder;
    }

    void mapFile() throws IOException
    {
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public HeapMemory allocateHeapPage()
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public DirectMemory getUninitiatedDirectMemoryPage()
    {
        return MemoryFactory.getUninitiatedDirectMemory(pageSize, fileByteOrder);
    }

    @Override
    public @ByteSize long getPageSize()
    {
        return pageSize;
    }

    @Override
    public ByteOrder getOrder()
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public @PageNumber long getPageNumber(final @ByteOffset long offset)
    {
        return StorageUnits.pageNumber(offset / pageSize);
    }

    @Override
    public @ByteOffset long getOffset(final @PageNumber long pageNumber)
    {
        return StorageUnits.offset(pageNumber * pageSize);
    }

    @Override
    public @ByteOffset long append(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public @ByteOffset long append(byte[] buffer)
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public @PageNumber long appendPageAligned(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public void flush(final boolean flushMeta)
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public void commitMetadata(@ByteOffset long lastPersistedOffset, @Version long version)
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public @ByteOffset long getLastPersistedOffset()
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public @PageNumber long getLastPersistedPageNumber()
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public @Version long getAppendVersion()
    {
        throw new UnsupportedOperationException("Method not Implemented");
    }

    @Override
    public void mapPage(final @PageNumber long pageNumber, final DirectMemory memory)
    {
        assert pageNumber > 0 : "page number must be > 0, provided " + pageNumber;

        final @ByteOffset long pageOffset = MemoryFactory.getPageOffset(mappedByteBuffer, getOffset(pageNumber));
        memory.setBaseAddress(pageOffset);
    }

    @Override
    public void readBytes(@ByteOffset long offset, ByteBuffer destinationBuffer)
    {
        mappedByteBuffer.position((int)offset);
        mappedByteBuffer.get(destinationBuffer.array());
    }
}
