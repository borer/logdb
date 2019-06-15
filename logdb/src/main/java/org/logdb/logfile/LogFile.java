package org.logdb.logfile;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

public class LogFile
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

    public @ByteOffset long put(final byte[] key, final byte[] value)
    {
        version++;
        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final @ByteOffset long recordStartOffset = logRecordStorage.write(key, value, version, timestamp);

        storage.commitMetadata(recordStartOffset, version);
        storage.flush();
        return recordStartOffset;
    }

    public byte[] read(final @ByteOffset long offset)
    {
        return logRecordStorage.readRecordValue(offset);
    }

    public void close() throws Exception
    {
        storage.close();
    }

    public @ByteOffset long remove(final byte[] key)
    {
        version++;
        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final @ByteOffset long offset = logRecordStorage.writeDelete(key, version, timestamp);

        storage.commitMetadata(offset, version);
        storage.flush();

        return offset;
    }
}
