package org.logdb.storage.file;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.Version;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

public interface FileHeader
{
    void writeHeadersAndAlign(SeekableByteChannel channel) throws IOException;

    void writeStaticHeaderTo(SeekableByteChannel channel) throws IOException;

    void writeDynamicHeaderTo(SeekableByteChannel channel) throws IOException;

    @ByteSize long getSegmentFileSize();

    @ByteSize int getPageSize();

    ByteOrder getOrder();

    @Version int getDbVersion();

    @ByteOffset long getGlobalAppendOffset();

    @ByteOffset long getCurrentFileAppendOffset();

    @Version long getAppendVersion();

    void updateMeta(
            @ByteOffset long globalAppendOffsetOffset,
            @ByteOffset long currentFileAppendOffset,
            @Version long appendVersion);

    void flush(boolean flushMeta) throws IOException;
}
