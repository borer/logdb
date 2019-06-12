package org.logdb.logfile;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogRecordHeaderTest
{
    private static final int CHECKSUM = 1;
    private static final int KEY_LENGTH = 2;
    private static final int VALUE_LENGTH = 3;
    private static final long VERSION = 4L;
    private static final long TIMESTAMP = 5L;

    private LogRecordHeader logRecordHeader;
    private ByteBuffer buffer;

    @BeforeEach
    void setUp()
    {
        logRecordHeader = new LogRecordHeader();
        buffer = ByteBuffer.allocate(LogRecordHeader.RECORD_HEADER_SIZE);
    }

    @Test
    void shouldBeAbleToWriteAndReadPutHeader()
    {
        logRecordHeader.initPut(CHECKSUM, KEY_LENGTH, VALUE_LENGTH, VERSION, TIMESTAMP);
        logRecordHeader.write(buffer);

        final LogRecordHeader readLogRecordHeader = new LogRecordHeader();
        readLogRecordHeader.read(buffer);

        assertLogRecordHeader(readLogRecordHeader);
    }

    @Test
    void shouldBeAbleToWriteAndReadDeleteHeader()
    {
        logRecordHeader.initDelete(CHECKSUM, KEY_LENGTH, VERSION, TIMESTAMP);
        logRecordHeader.write(buffer);

        final LogRecordHeader readLogRecordHeader = new LogRecordHeader();
        readLogRecordHeader.read(buffer);

        assertLogRecordHeader(readLogRecordHeader);
    }

    private void assertLogRecordHeader(LogRecordHeader readLogRecordHeader)
    {
        assertEquals(logRecordHeader.getChecksum(), readLogRecordHeader.getChecksum());
        assertEquals(logRecordHeader.getRecordType(), readLogRecordHeader.getRecordType());
        assertEquals(logRecordHeader.getKeyLength(), readLogRecordHeader.getKeyLength());
        assertEquals(logRecordHeader.getValueLength(), readLogRecordHeader.getValueLength());
        assertEquals(logRecordHeader.getTimestamp(), readLogRecordHeader.getTimestamp());
        assertEquals(logRecordHeader.getVersion(), readLogRecordHeader.getVersion());
    }
}