package org.logdb.bit;

public interface DirectMemory
{
    void setBaseAddress(long baseAddress);

    Memory toMemory();
}
