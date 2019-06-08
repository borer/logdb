package org.logdb.logfile;

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

    @Test
    void shouldBeAbleToWriteAndRead()
    {
        final LogRecordHeader logRecordHeader = new LogRecordHeader();
        logRecordHeader.init(CHECKSUM, KEY_LENGTH, VALUE_LENGTH, VERSION, TIMESTAMP);

        final ByteBuffer buffer = ByteBuffer.allocate(LogRecordHeader.RECORD_HEADER_SIZE);
        logRecordHeader.write(buffer);

        final LogRecordHeader readLogRecordHeader = new LogRecordHeader();
        readLogRecordHeader.read(buffer);

        assertEquals(logRecordHeader.getChecksum(), readLogRecordHeader.getChecksum());
        assertEquals(logRecordHeader.getKeyLength(), readLogRecordHeader.getKeyLength());
        assertEquals(logRecordHeader.getValueLength(), readLogRecordHeader.getValueLength());
        assertEquals(logRecordHeader.getTimestamp(), readLogRecordHeader.getTimestamp());
        assertEquals(logRecordHeader.getVersion(), readLogRecordHeader.getVersion());
    }
}