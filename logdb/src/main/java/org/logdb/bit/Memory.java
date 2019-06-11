package org.logdb.bit;

import org.logdb.storage.ByteOffset;

public interface Memory extends ReadMemory, WriteMemory
{
    @ByteOffset long getBaseAddress();

    void resetPosition();

    long getCapacity();

    void assertBounds(@ByteOffset long requestOffset, int requestLength);

    void assertBounds(@ByteOffset long requestOffset, long requestLength);
}
