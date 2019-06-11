package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;

public interface Memory extends ReadMemory, WriteMemory
{
    @ByteOffset long getBaseAddress();

    void resetPosition();

    @ByteSize long getCapacity();

    void assertBounds(@ByteOffset long requestOffset, @ByteSize int requestLength);

    void assertBounds(@ByteOffset long requestOffset, @ByteSize long requestLength);
}
