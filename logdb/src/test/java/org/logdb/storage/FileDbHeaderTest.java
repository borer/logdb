package org.logdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileDbHeaderTest
{
    @Test
    void shouldBeAbleToSaveAndLoadHeader()
    {
        final FileDbHeader expectedHeader = new FileDbHeader(
                ByteOrder.BIG_ENDIAN,
                123,
                4096,
                4096 << 5,
                987654321L);

        final ByteBuffer buffer = ByteBuffer.allocate(FileDbHeader.getSizeBytes());

        expectedHeader.writeTo(buffer);
        buffer.rewind();

        final FileDbHeader actualHeader = FileDbHeader.readFrom(buffer);

        assertEquals(expectedHeader, actualHeader);
    }
}