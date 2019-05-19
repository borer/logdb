package org.logdb.bit;

public interface Memory extends ReadMemory, WriteMemory
{
    void assertBounds(long requestOffset, int requestLength);

    void assertBounds(long requestOffset, long requestLength);
}
