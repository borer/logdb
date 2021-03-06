package org.logdb.logfile;

import org.junit.jupiter.api.Test;
import org.logdb.checksum.ChecksumHelper;
import org.logdb.checksum.ChecksumType;
import org.logdb.checksum.Crc32;
import org.logdb.storage.memory.MemoryStorage;
import org.logdb.support.TestUtils;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.support.TestUtils.MEMORY_CHUNK_SIZE;

class LogRecordStorageTest
{
    @Test
    void shouldPersistAndReadRecord() throws IOException
    {
        final MemoryStorage storage = new MemoryStorage(TestUtils.BYTE_ORDER, TestUtils.PAGE_SIZE_BYTES, MEMORY_CHUNK_SIZE);
        final ChecksumHelper checksumHelper = new ChecksumHelper(new Crc32(), ChecksumType.CRC32);
        final LogRecordStorage logRecordStorage = new LogRecordStorage(storage, checksumHelper);

        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();

        final long positionOffset = logRecordStorage.writePut(key, value, 1, 2);

        assertEquals(0, positionOffset);
        assertArrayEquals(value, logRecordStorage.readValue(positionOffset));
    }
}