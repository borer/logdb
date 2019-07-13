package org.logdb.root.index;

import org.junit.jupiter.api.Test;
import org.logdb.bbtree.VersionNotFoundException;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.storage.memory.MemoryStorage;
import org.logdb.support.TestUtils;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.logdb.support.TestUtils.MEMORY_CHUNK_SIZE;

class RootIndexTest
{
    @Test
    void shouldBeAbleToPersistRecords() throws Exception
    {
        final MemoryStorage storage = new MemoryStorage(TestUtils.BYTE_ORDER, TestUtils.PAGE_SIZE_BYTES, MEMORY_CHUNK_SIZE);
        final @Version long version = StorageUnits.INITIAL_VERSION;
        final @Milliseconds long timestamp = TimeUnits.millis(1237123571L);
        final @ByteOffset long offset = StorageUnits.offset(12312313L);
        try (RootIndex rootIndex = new RootIndex(storage, version, timestamp, offset))
        {
            final int maxVersions = 10;
            for (int i = 0; i < maxVersions; i++)
            {
                rootIndex.append(version + i, timestamp + i, offset + i);
                rootIndex.commit();
            }

            for (int i = 0; i < maxVersions; i++)
            {
                assertEquals(offset + i, rootIndex.getVersionOffset(version + i));
            }

            assertEquals(offset + maxVersions - 1, rootIndex.getTimestampOffset(timestamp + maxVersions - 1));
        }
    }

    @Test
    void shouldFailToFindNonExistingVersion() throws Exception
    {
        final MemoryStorage storage = new MemoryStorage(TestUtils.BYTE_ORDER, TestUtils.PAGE_SIZE_BYTES, MEMORY_CHUNK_SIZE);
        final @Version long version = StorageUnits.INITIAL_VERSION;
        final @Milliseconds long timestamp = TimeUnits.millis(1237123571L);
        final @ByteOffset long offset = StorageUnits.offset(12312313L);
        try (RootIndex rootIndex = new RootIndex(storage, version, timestamp, offset))
        {
            rootIndex.append(version, timestamp, offset);
            rootIndex.commit();

            try
            {
                rootIndex.getVersionOffset(-1);
                fail("should not execute");
            }
            catch (final VersionNotFoundException e)
            {
                assertEquals("The version -1 was not found.", e.getMessage());
            }

            try
            {
                rootIndex.getVersionOffset(version + 1);
                fail("should not execute");
            }
            catch (final VersionNotFoundException e)
            {
                assertEquals("The version 1 was not found.", e.getMessage());
            }
        }
    }
}