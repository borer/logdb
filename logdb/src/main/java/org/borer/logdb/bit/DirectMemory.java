package org.borer.logdb.bit;

public interface DirectMemory
{
    void setBaseAddress(long baseAddress);

    Memory toMemory();
}
