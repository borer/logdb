package org.logdb.logfile;

import org.logdb.storage.Storage;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

import java.io.IOException;

public class LogFile
{
    private final LogRecordStorage logRecordStorage;
    private final Storage storage;
    private final TimeSource timeSource;

    private int version;

    public LogFile(final Storage storage, final TimeSource timeSource)
    {
        this.storage = storage;
        this.timeSource = timeSource;
        this.version = 0;
        this.logRecordStorage = new LogRecordStorage(storage);
    }

    public long put(final byte[] key, final byte[] value)
    {
        version++;

        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final long recordStartOffset = logRecordStorage.write(key, value, version, timestamp);

        storage.flush();

        return recordStartOffset;
    }

    public byte[] read(final long offset)
    {
        return logRecordStorage.readRecordValue(offset);
    }

    public void close() throws IOException
    {
        storage.close();
    }
}
