package org.logdb.bit;

import org.logdb.storage.ByteOffset;

public interface DirectMemory extends Memory
{
    void setBaseAddress(@ByteOffset long baseAddress);

    boolean isUninitialized();
}
