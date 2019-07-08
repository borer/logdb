package org.logdb.root.index;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;

public class RootIndex implements AutoCloseable
{
    private final Storage storage;
    private final RootIndexRecord rootIndexRecord;

    private @Version long lastVersion;
    private @Milliseconds long lastTimestamp;
    private @ByteOffset long lastOffset;

    public RootIndex(final Storage storage,
                     final @Version long version,
                     final @Milliseconds long timestamp,
                     final @ByteOffset long offset)
    {
        this.storage = storage;
        this.rootIndexRecord = new RootIndexRecord();

        rootIndexRecord.set(version, timestamp, offset);
        set(version, timestamp, offset);
    }

    public void append(
            final @Version long version,
            final @Milliseconds long timestamp,
            final @ByteOffset long offset) throws IOException
    {
        rootIndexRecord.set(version, timestamp, offset);
        final @ByteOffset long globalOffset = storage.append(rootIndexRecord.getBuffer());

        set(version, timestamp, offset);

        storage.commitMetadata(globalOffset, version);
    }

    private void set(
            final @Version long version,
            final @Milliseconds long timestamp,
            final @ByteOffset long offset)
    {
        this.lastVersion = version;
        this.lastTimestamp = timestamp;
        this.lastOffset = offset;
    }

    public void commit()
    {
        storage.flush();
    }

    public @ByteOffset long getVersionOffset(final @Version long version)
    {
        if (version == lastVersion)
        {
            return lastOffset;
        }

        return StorageUnits.ZERO_OFFSET;
    }

    public @ByteOffset long getTimestampOffset(final @Milliseconds long timestamp)
    {
        if (timestamp == lastTimestamp)
        {
            return lastOffset;
        }

        return StorageUnits.ZERO_OFFSET;
    }

    @Override
    public void close() throws Exception
    {
        storage.close();
    }
}
