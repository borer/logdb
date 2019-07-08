package org.logdb.root.index;

import org.junit.jupiter.api.Test;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.storage.memory.MemoryStorage;
import org.logdb.support.TestUtils;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RootIndexTest
{
    @Test
    void shouldBeAbleToPersistRecords() throws Exception
    {
        final MemoryStorage storage = new MemoryStorage(TestUtils.BYTE_ORDER, TestUtils.PAGE_SIZE_BYTES);
        final @Version long version = StorageUnits.version(1L);
        final @Milliseconds long timestamp = TimeUnits.millis(1237123571L);
        final @ByteOffset long offset = StorageUnits.offset(12312313L);
        try (RootIndex rootIndex = new RootIndex(storage, version, timestamp, offset))
        {
            rootIndex.append(version, timestamp, offset);

            assertEquals(offset, rootIndex.getVersionOffset(version));
            assertEquals(offset, rootIndex.getTimestampOffset(timestamp));
        }
    }
}