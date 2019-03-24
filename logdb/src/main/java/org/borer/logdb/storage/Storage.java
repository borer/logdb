package org.borer.logdb.storage;

import org.borer.logdb.bit.Memory;

public interface Storage
{
    Memory allocateWritableMemory();

    void returnWritableMemory(Memory writableMemory);

    long commitNode(Memory node);

    void flush();

    void commitMetadata(long lastRootPageNumber);

    Memory loadLastRoot();
}
