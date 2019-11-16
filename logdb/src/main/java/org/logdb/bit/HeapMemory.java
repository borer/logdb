package org.logdb.bit;

import java.nio.ByteBuffer;

public interface HeapMemory extends Memory
{
    byte[] getArray();

    ByteBuffer getSupportByteBuffer();
}
