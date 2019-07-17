package org.logdb.root.index;

import org.logdb.bbtree.VersionNotFoundException;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

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
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.rootIndexRecord = new RootIndexRecord(storage.getOrder(), version, timestamp, offset);

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

    public void flush(final boolean flushMeta)
    {
        storage.flush(flushMeta);
    }

    public @ByteOffset long getVersionOffset(final @Version long version)
    {
        if (version < 0 || version > lastVersion)
        {
            throw new VersionNotFoundException(version);
        }

        if (version == lastVersion)
        {
            return lastOffset;
        }
        else
        {
            final @ByteOffset long offset = StorageUnits.offset(version * RootIndexRecord.SIZE);
            final ByteBuffer buffer = ByteBuffer.allocate(RootIndexRecord.SIZE);
            buffer.order(storage.getOrder());
            storage.readBytes(offset, buffer);

            return RootIndexRecord.readOffsetValue(buffer);
        }
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
