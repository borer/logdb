package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;

import java.nio.ByteOrder;

public interface Memory extends ReadMemory, WriteMemory
{
    @ByteOffset long getBaseAddress();

    ByteOrder getByteOrder();

    void resetPosition();

    @ByteSize long getCapacity();

    Memory slice(@ByteOffset int startOffset);

    Memory sliceRange(@ByteOffset int startOffset, @ByteOffset int endOffset);

    void assertBounds(@ByteOffset long requestOffset, @ByteSize int requestLength);

    void assertBounds(@ByteOffset long requestOffset, @ByteSize long requestLength);
}
