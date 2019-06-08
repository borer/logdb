package org.logdb.logfile;

import org.logdb.storage.Storage;

public class LogFile
{
    private final LogRecordStorage logRecordStorage;
    private final Storage storage;
    private int version;

    public LogFile(final Storage storage)
    {
        this.storage = storage;
        this.version = 0;
        this.logRecordStorage = new LogRecordStorage(storage);
    }

    public long put(final byte[] key, final byte[] value)
    {
        version++;

        final long timestamp = System.currentTimeMillis();

        final long recordStartOffset = logRecordStorage.write(key, value, version, timestamp);

        storage.flush();

        return recordStartOffset;
    }

    public byte[] read(final long offset)
    {
        return logRecordStorage.readRecordValue(offset);
    }
}
