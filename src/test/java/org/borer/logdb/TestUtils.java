package org.borer.logdb;

import java.nio.ByteBuffer;

public class TestUtils
{
    static ByteBuffer createValue(final String value)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(value.length());
        buffer.put(value.getBytes());
        buffer.rewind();

        return buffer;
    }
}