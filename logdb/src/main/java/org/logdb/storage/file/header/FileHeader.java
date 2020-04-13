package org.logdb.storage.file.header;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.Version;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

public interface FileHeader
{
    void writeAlign(SeekableByteChannel channel) throws IOException;

    void writeStaticHeaderTo(SeekableByteChannel channel) throws IOException;

    void writeDynamicHeaderTo(SeekableByteChannel channel) throws IOException;

    @ByteSize long getSegmentFileSize();

    @ByteSize int getPageSize();

    @ByteSize int getPageLogSize();

    ByteOrder getOrder();

    @ByteSize int getChecksumSize();

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
