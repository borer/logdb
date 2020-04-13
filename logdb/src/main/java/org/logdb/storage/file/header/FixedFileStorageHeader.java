package org.logdb.storage.file.header;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
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

    public FixedFileStorageHeader(
            final FileHeader delegate,
            final RandomAccessFile headerFile,
            final FileChannel headerFileChannel)
    {
        this.delegate = delegate;
        this.headerFile = headerFile;
        this.headerFileChannel = headerFileChannel;
    }

    @Override
    public void writeAlign(final SeekableByteChannel channel) throws IOException
    {
        delegate.writeAlign(headerFileChannel);
    }

    @Override
    public void writeDynamicHeaderTo(final SeekableByteChannel channel) throws IOException
    {
        delegate.writeDynamicHeaderTo(headerFileChannel);
    }

    @Override
    public void writeStaticHeaderTo(SeekableByteChannel channel) throws IOException
    {
        delegate.writeStaticHeaderTo(headerFileChannel);
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
    public @ByteSize int getPageLogSize()
    {
        return delegate.getPageLogSize();
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
    public @ByteOffset long getCurrentFileAppendOffset()
    {
        return delegate.getCurrentFileAppendOffset();
    }

    @Override
    public @Version long getAppendVersion()
    {
        return delegate.getAppendVersion();
    }

    @Override
    public @ByteSize int getChecksumSize()
    {
        return delegate.getChecksumSize();
    }

    @Override
    public void updateMeta(
            final @ByteOffset long globalAppendOffsetOffset,
            final @ByteOffset long currentFileAppendOffset,
            final @Version long appendVersion)
    {
        delegate.updateMeta(globalAppendOffsetOffset, currentFileAppendOffset, appendVersion);
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
