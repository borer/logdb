package org.logdb.logfile;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.Storage;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

import java.io.IOException;

public class LogFile implements AutoCloseable
{
    private final LogRecordStorage logRecordStorage;
    private final Storage storage;
    private final TimeSource timeSource;

    private @Version long nextWriteVersion;

    public LogFile(final Storage storage, final TimeSource timeSource, final @Version long nextWriteVersion)
    {
        this.storage = storage;
        this.timeSource = timeSource;
        this.nextWriteVersion = nextWriteVersion;
        this.logRecordStorage = new LogRecordStorage(storage);
    }

    public @ByteOffset long put(final byte[] key, final byte[] value) throws IOException
    {
        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final @ByteOffset long putRecordStartOffset = logRecordStorage.writePut(
                key,
                value,
                nextWriteVersion,
                timestamp);

        storage.commitMetadata(putRecordStartOffset, nextWriteVersion);
        storage.flush();
        nextWriteVersion++;

        return putRecordStartOffset;
    }

    public byte[] read(final @ByteOffset long offset)
    {
        return logRecordStorage.readRecordValue(offset);
    }

    public @ByteOffset long delete(final byte[] key) throws IOException
    {
        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final @ByteOffset long deleteRecordStartOffset = logRecordStorage.writeDelete(
                key,
                nextWriteVersion,
                timestamp);

        storage.commitMetadata(deleteRecordStartOffset, nextWriteVersion);
        storage.flush();
        nextWriteVersion++;

        return deleteRecordStartOffset;
    }

    @Override
    public void close() throws Exception
    {
        storage.close();
    }
}
