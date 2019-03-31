package org.borer.logdb.storage;

import org.borer.logdb.bit.Memory;

/**
 * Storage interface for the underlying persistence of the btree.
 * <p>
 *     A storage normally is split in 2 types:
 *  <ul>
 *      <li>
 *          Writable memory, that is used only temporary for dirty nodes and is returned to the pool
 *          once the node is been written to persistent storage
 *      </li>
 *      <li>
 *          ReadOnly memory, returned by the load methods.
 *          Normally is read only memory mapped region of the underlying persistent storage.
 *      </li>
 *  </ul>
 *  </p>
 */
public interface Storage
{
    Memory allocateWritableMemory();

    void returnWritableMemory(Memory writableMemory);

    long commitNode(Memory node);

    void flush();

    void commitMetadata(long lastRootPageNumber);

    long getLastRootPageNumber();

    Memory loadLastRoot();

    Memory loadPage(long pageOffset);
}
