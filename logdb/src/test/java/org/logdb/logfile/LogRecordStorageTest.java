package org.logdb.logfile;

import org.junit.jupiter.api.Test;
import org.logdb.storage.MemoryStorage;
import org.logdb.support.TestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogRecordStorageTest
{
    @Test
    void shouldPersistAndReadRecord()
    {
        final MemoryStorage storage = new MemoryStorage(TestUtils.BYTE_ORDER, TestUtils.PAGE_SIZE_BYTES);
        final LogRecordStorage logRecordStorage = new LogRecordStorage(storage);

        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();

        final long positionOffset = logRecordStorage.write(key, value, 1, 2);

        assertEquals(0, positionOffset);
    }
}