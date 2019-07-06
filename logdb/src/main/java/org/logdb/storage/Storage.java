package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
public interface Storage extends AutoCloseable
{
    HeapMemory allocateHeapPage();

    DirectMemory getUninitiatedDirectMemoryPage();

    @ByteSize long getPageSize();

    ByteOrder getOrder();

    @ByteOffset long getLastPersistedOffset();

    @Version long getAppendVersion();

    @PageNumber long getPageNumber(@ByteOffset long offset);

    @ByteOffset long getOffset(@PageNumber long pageNumber);

    /**
     * Write any arbitrary buffer.
     * @param buffer the buffer to store. Can be any size
     * @return the byte offset where the buffer start is located.
     */
    @ByteOffset long append(ByteBuffer buffer) throws IOException;

    /**
     * Writes a page aligned bytebuffer.
     * @param buffer the buffer to store, must be of size of a page
     * @return the page number where the buffer is located
     */
    @PageNumber long appendPageAligned(ByteBuffer buffer) throws IOException;

    void commitMetadata(@ByteOffset long lastPersistedOffset, @Version long version);

    /**
     * Maps the page specified by pageNumber into memory object.
     * @param pageNumber the page number to map
     * @param memory the memory that is going to point to the start of the page
     */
    void mapPage(@PageNumber long pageNumber, DirectMemory memory);

    void flush();
}
