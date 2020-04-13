package org.logdb.storage.file.header;

import org.logdb.checksum.ChecksumHelper;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.Version;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

public class FileStorageHeader implements FileHeader
{
    private final FileStorageStaticHeader fileStorageStaticHeader;
    private final FileStorageDynamicHeader fileStorageDynamicHeader;

    public FileStorageHeader(final FileStorageStaticHeader fileStorageStaticHeader, final FileStorageDynamicHeader fileStorageDynamicHeader)
    {
        this.fileStorageStaticHeader = fileStorageStaticHeader;
        this.fileStorageDynamicHeader = fileStorageDynamicHeader;
    }

    public static FileHeader newHeader(
            final ByteOrder byteOrder,
            final @ByteSize int pageSizeBytes,
            final @ByteSize int pageLogSize,
            final @ByteSize long segmentFileSize,
            final ChecksumHelper checksumHelper)
    {
        final FileStorageStaticHeader staticHeader = FileStorageStaticHeader.newHeader(
                byteOrder,
                pageSizeBytes,
                pageLogSize,
                segmentFileSize,
                checksumHelper.getType());
        final FileStorageDynamicHeader dynamicHeader = FileStorageDynamicHeader.newHeader(pageSizeBytes, checksumHelper);

        return new FileStorageHeader(staticHeader, dynamicHeader);
    }

    public static FileStorageHeader readFrom(final SeekableByteChannel headerChannel) throws IOException
    {
        final FileStorageStaticHeader staticHeader = FileStorageStaticHeader.readFrom(headerChannel);
        final FileStorageDynamicHeader dynamicHeader = FileStorageDynamicHeader.readFrom(
                headerChannel,
                staticHeader.getChecksumType(),
                staticHeader.getPageSize());

        return new FileStorageHeader(staticHeader, dynamicHeader);
    }

    @Override
    public void writeAlign(final SeekableByteChannel channel) throws IOException
    {
        fileStorageStaticHeader.writeAlign(channel);
        fileStorageDynamicHeader.writeAlign(channel);
    }

    @Override
    public void writeStaticHeaderTo(final SeekableByteChannel channel) throws IOException
    {
        fileStorageStaticHeader.write(channel);
    }

    @Override
    public void writeDynamicHeaderTo(final SeekableByteChannel channel) throws IOException
    {
        fileStorageDynamicHeader.write(channel);
    }

    @Override
    public @ByteSize long getSegmentFileSize()
    {
        return fileStorageStaticHeader.getSegmentFileSize();
    }

    @Override
    public @ByteSize int getPageSize()
    {
        return fileStorageStaticHeader.getPageSize();
    }

    @Override
    public @ByteSize int getPageLogSize()
    {
        return fileStorageStaticHeader.getPageLogSize();
    }

    @Override
    public ByteOrder getOrder()
    {
        return fileStorageStaticHeader.getOrder();
    }

    @Override
    public @Version int getDbVersion()
    {
        return fileStorageStaticHeader.getDbVersion();
    }

    @Override
    public @ByteSize int getChecksumSize()
    {
        return fileStorageDynamicHeader.getChecksumSize();
    }

    @Override
    public @ByteOffset long getGlobalAppendOffset()
    {
        return fileStorageDynamicHeader.getGlobalAppendOffset();
    }

    @Override
    public @ByteOffset long getCurrentFileAppendOffset()
    {
        return fileStorageDynamicHeader.getCurrentFileAppendOffset();
    }

    @Override
    public @Version long getAppendVersion()
    {
        return fileStorageDynamicHeader.getAppendVersion();
    }

    @Override
    public void updateMeta(@ByteOffset long globalAppendOffsetOffset, @ByteOffset long currentFileAppendOffset, @Version long appendVersion)
    {
        fileStorageDynamicHeader.updateMeta(globalAppendOffsetOffset, currentFileAppendOffset, appendVersion);
    }

    @Override
    public void flush(boolean flushMeta) throws IOException
    {
        // NO-OP
    }
}
