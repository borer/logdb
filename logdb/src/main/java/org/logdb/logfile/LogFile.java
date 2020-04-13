package org.logdb.logfile;

import org.logdb.checksum.ChecksumHelper;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.Storage;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

import java.io.IOException;

public class LogFile implements AutoCloseable
{
    //TODO: LogRecordStorage is currently single threaded, think about ways to improve it
    private final LogRecordStorage writeLogRecordStorage;
    private final LogRecordStorage readLogRecordStorage;
    private final boolean shouldSyncWrite;
    private final Storage storage;
    private final TimeSource timeSource;

    private @Version long nextWriteVersion;

    public LogFile(
            final Storage storage,
            final TimeSource timeSource,
            final @Version long nextWriteVersion,
            final boolean shouldSyncWrite,
            final ChecksumHelper checksumHelper)
    {
        this.storage = storage;
        this.timeSource = timeSource;
        this.nextWriteVersion = nextWriteVersion;
        this.writeLogRecordStorage = new LogRecordStorage(storage, checksumHelper);
        this.readLogRecordStorage = new LogRecordStorage(storage, checksumHelper);
        this.shouldSyncWrite = shouldSyncWrite;
    }

    public @ByteOffset long put(final byte[] key, final byte[] value) throws IOException
    {
        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final @ByteOffset long putRecordStartOffset = writeLogRecordStorage.writePut(
                key,
                value,
                nextWriteVersion,
                timestamp);

        storage.commitMetadata(putRecordStartOffset, nextWriteVersion);
        flushStorage();

        nextWriteVersion++;

        return putRecordStartOffset;
    }

    public byte[] read(final @ByteOffset long offset)
    {
        return readLogRecordStorage.readValue(offset);
    }

    public @ByteOffset long delete(final byte[] key) throws IOException
    {
        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final @ByteOffset long deleteRecordStartOffset = writeLogRecordStorage.writeDelete(
                key,
                nextWriteVersion,
                timestamp);

        storage.commitMetadata(deleteRecordStartOffset, nextWriteVersion);
        flushStorage();
        nextWriteVersion++;

        return deleteRecordStartOffset;
    }

    @Override
    public void close() throws Exception
    {
        storage.close();
    }

    private void flushStorage()
    {
        if (shouldSyncWrite)
        {
            storage.flush(false);
        }
    }
}
