package org.logdb.root.index;

import org.logdb.bit.DirectMemory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;
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
        else
        {
            //TODO: don't allocate direct memory allocation
            final DirectMemory directMemory = storage.getUninitiatedDirectMemoryPage();
            final @ByteOffset long offset = StorageUnits.offset(version * RootIndexRecord.SIZE);
            final @PageNumber long pageNumber = storage.getPageNumber(offset);
            storage.mapPage(pageNumber, directMemory);

            final @ByteOffset long offsetInsidePage = offset - storage.getOffset(pageNumber);
            return RootIndexRecord.readOffsetValue(directMemory, offsetInsidePage);
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
