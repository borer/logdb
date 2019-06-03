package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;

import java.io.Closeable;
import java.nio.ByteBuffer;

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
    HeapMemory allocateHeapPage();

    DirectMemory getUninitiatedDirectMemoryPage();

    long getPageSize();

    /**
     * Write any arbitrary buffer.
     * @param buffer the buffer to store. Can be any size
     * @return the byte offset where the buffer start is located.
     */
    long write(ByteBuffer buffer);

    /**
     * Writes a page aligned bytebuffer.
     * @param buffer the buffer to store, must be of size of a page
     * @return the page number where the buffer is located
     */
    long writePageAligned(ByteBuffer buffer);

    void flush();

    void commitMetadata(long lastRootPageNumber, long version);

    long getLastRootPageNumber();

    DirectMemory loadPage(long pageNumber);

    long getBaseOffsetForPageNumber(long pageNumber);
}
