package org.logdb.logfile;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

import java.io.IOException;

public class LogFile implements AutoCloseable
{
    private final LogRecordStorage logRecordStorage;
    private final Storage storage;
    private final TimeSource timeSource;

    private @Version long version;

    public LogFile(final Storage storage, final TimeSource timeSource)
    {
        this.storage = storage;
        this.timeSource = timeSource;
        this.version = StorageUnits.version(0);
        this.logRecordStorage = new LogRecordStorage(storage);
    }

    public @ByteOffset long put(final byte[] key, final byte[] value) throws IOException
    {
        version++;
        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final @ByteOffset long putRecordStartOffset = logRecordStorage.writePut(
                key,
                value,
                version,
                timestamp);

        final @ByteSize long putRecordSize = logRecordStorage.getPutRecordSize(key, value);
        final @ByteOffset long putRecordEndOffset = StorageUnits.offset(putRecordStartOffset + putRecordSize);

        storage.commitMetadata(putRecordEndOffset, version);
        storage.flush();

        return putRecordStartOffset;
    }

    public byte[] read(final @ByteOffset long offset)
    {
        return logRecordStorage.readRecordValue(offset);
    }

    public @ByteOffset long delete(final byte[] key) throws IOException
    {
        version++;
        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final @ByteOffset long deleteRecordStartOffset = logRecordStorage.writeDelete(
                key,
                version,
                timestamp);

        final @ByteSize long deleteRecordSize = logRecordStorage.getDeleteRecordSize(key);
        final @ByteOffset long deleteRecordEndOffset = StorageUnits.offset(deleteRecordStartOffset + deleteRecordSize);

        storage.commitMetadata(deleteRecordEndOffset, version);
        storage.flush();

        return deleteRecordStartOffset;
    }

    @Override
    public void close() throws Exception
    {
        storage.close();
    }
}
