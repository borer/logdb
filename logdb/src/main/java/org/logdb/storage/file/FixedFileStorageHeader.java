package org.logdb.storage.file;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;

public class FixedFileStorageHeader implements FileHeader, AutoCloseable
{
    private final FileHeader delegate;
    private final RandomAccessFile headerFile;
    private final FileChannel headerFileChannel;

    FixedFileStorageHeader(
            final FileHeader delegate,
            final RandomAccessFile headerFile,
            final FileChannel headerFileChannel)
    {
        this.delegate = delegate;
        this.headerFile = headerFile;
        this.headerFileChannel = headerFileChannel;
    }

    @Override
    public void writeToAndPageAlign(final SeekableByteChannel channel) throws IOException
    {
        headerFileChannel.position(StorageUnits.ZERO_OFFSET);

        delegate.writeToAndPageAlign(headerFileChannel);
    }

    @Override
    public void writeTo(final SeekableByteChannel channel) throws IOException
    {
        headerFileChannel.position(StorageUnits.ZERO_OFFSET);

        delegate.writeTo(headerFileChannel);
    }

    @Override
    public @ByteSize long getHeaderSizeAlignedToNearestPage()
    {
        return delegate.getHeaderSizeAlignedToNearestPage();
    }

    @Override
    public @ByteSize long getSegmentFileSize()
    {
        return delegate.getSegmentFileSize();
    }

    @Override
    public @ByteSize int getPageSize()
    {
        return delegate.getPageSize();
    }

    @Override
    public ByteOrder getOrder()
    {
        return delegate.getOrder();
    }

    @Override
    public @Version int getDbVersion()
    {
        return delegate.getDbVersion();
    }

    @Override
    public @ByteOffset long getGlobalAppendOffset()
    {
        return delegate.getGlobalAppendOffset();
    }

    @Override
    public @ByteOffset long getLastFileAppendOffset()
    {
        return delegate.getLastFileAppendOffset();
    }

    @Override
    public @Version long getAppendVersion()
    {
        return delegate.getAppendVersion();
    }

    @Override
    public void updateMeta(
            final @ByteOffset long lastPersistedOffset,
            final @ByteOffset long appendOffset,
            final @Version long appendVersion)
    {
        delegate.updateMeta(lastPersistedOffset, appendOffset, appendVersion);
    }

    @Override
    public void flush(final boolean flushMeta) throws IOException
    {
        headerFileChannel.force(flushMeta);
    }

    @Override
    public String toString()
    {
        return "FixedFileStorageHeader{" +
                "delegate=" + delegate +
                '}';
    }

    @Override
    public void close() throws Exception
    {
        headerFile.close();
    }
}
