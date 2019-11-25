package org.logdb.logfile;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LogRecordHeaderTest
{
    private static final byte[] CHECKSUM = {80, 80};
    private static final int KEY_LENGTH = 2;
    private static final int VALUE_LENGTH = 3;
    private static final long VERSION = 4L;
    private static final long TIMESTAMP = 5L;

    private LogRecordHeader logRecordHeader;
    private ByteBuffer buffer;

    @BeforeEach
    void setUp()
    {
        logRecordHeader = new LogRecordHeader(CHECKSUM.length);
        final int checksumSize = Integer.BYTES + CHECKSUM.length;
        buffer = ByteBuffer.allocate(LogRecordHeader.RECORD_HEADER_STATIC_SIZE + checksumSize);
    }

    @Test
    void shouldBeAbleToWriteAndReadPutHeader()
    {
        logRecordHeader.initPut(CHECKSUM, KEY_LENGTH, VALUE_LENGTH, VERSION, TIMESTAMP);
        logRecordHeader.write(buffer);

        final LogRecordHeader readLogRecordHeader = new LogRecordHeader(CHECKSUM.length);
        readLogRecordHeader.read(buffer);

        assertLogRecordHeader(readLogRecordHeader);
    }

    @Test
    void shouldBeAbleToWriteAndReadDeleteHeader()
    {
        logRecordHeader.initDelete(CHECKSUM, KEY_LENGTH, VERSION, TIMESTAMP);
        logRecordHeader.write(buffer);

        final LogRecordHeader readLogRecordHeader = new LogRecordHeader(CHECKSUM.length);
        readLogRecordHeader.read(buffer);

        assertLogRecordHeader(readLogRecordHeader);
    }

    private void assertLogRecordHeader(LogRecordHeader readLogRecordHeader)
    {
        assertArrayEquals(logRecordHeader.getChecksum(), readLogRecordHeader.getChecksum());
        assertEquals(logRecordHeader.getRecordType(), readLogRecordHeader.getRecordType());
        assertEquals(logRecordHeader.getKeyLength(), readLogRecordHeader.getKeyLength());
        assertEquals(logRecordHeader.getValueLength(), readLogRecordHeader.getValueLength());
        assertEquals(logRecordHeader.getTimestamp(), readLogRecordHeader.getTimestamp());
        assertEquals(logRecordHeader.getVersion(), readLogRecordHeader.getVersion());
    }
}