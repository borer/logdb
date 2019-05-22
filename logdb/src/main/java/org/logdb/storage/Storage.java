package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.ReadMemory;

import java.io.Closeable;

/**
 * Storage interface for the underlying persistence of the btree.
 * <br>
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
 */
public interface Storage extends Closeable
{
    Memory allocateHeapMemory();

    DirectMemory getDirectMemory(long pageNumber);

    long getPageSize();

    long writeNode(ReadMemory node);

    void flush();

    void commitMetadata(long lastRootPageNumber);

    long getLastRootPageNumber();

    Memory loadPage(long pageNumber);

    long getBaseOffsetForPageNumber(long pageNumber);
}
