package org.logdb.storage.file;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.Version;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

public interface FileHeader
{
    void writeToAndPageAlign(SeekableByteChannel channel) throws IOException;

    void writeTo(SeekableByteChannel channel) throws IOException;

    @ByteSize long getHeaderSizeAlignedToNearestPage();

    @ByteSize long getSegmentFileSize();

    @ByteSize int getPageSize();

    ByteOrder getOrder();

    @Version int getDbVersion();

    @ByteOffset long getGlobalAppendOffset();

    @ByteOffset long getLastFileAppendOffset();

    @Version long getAppendVersion();

    void updateMeta(
            @ByteOffset long lastPersistedOffset,
            @ByteOffset long appendOffset,
            @Version long appendVersion);

    void flush(boolean flushMeta) throws IOException;
}
