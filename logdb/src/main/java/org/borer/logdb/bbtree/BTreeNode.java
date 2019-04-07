package org.borer.logdb.bbtree;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.storage.NodesManager;

public interface BTreeNode
{
    /**
     * The page number where this node is persisted or a generated id if it's not yet persisted.
      * @return pagenumber
     */
    long getPageNumber();

    /**
     * Gets the underlying buffer that stores the content of this node. Changes to that buffer will change the node content.
     * @return nodes buffer
     */
    Memory getBuffer();

    /**
     * Changes the underlying buffer to the new one. This is a dangerous non thread safe operation.
     * @param newBuffer the buffer to use for this node underlying content
     */
    void updateBuffer(Memory newBuffer);

    /**
     * Inserts key/value pair in the current leaf.
     * If the key already exits, its value is replaced.
     * @param key Key that identifies the value
     * @param value Value to persist
     */
    void insert(long key, long value);

    /**
     * Remove the key and value at index.
     * @param index the index of the element to remove
     */
    void remove(int index);

    /**
     * Get the number of entries in the leaf.
     * @return the number of entries.
     */
    int getKeyCount();

    /**
     * Returns a key at the given index.
     * @param index has to be between 0...getKeyCount()
     * @return the key
     */
    long getKey(int index);

    /**
     * Get the value corresponding to the key.
     * @param key key to search for
     * @return the value corresponding to that key or null if not found
     */
    long get(long key);

    /**
     * Get the key index corresponding to this key or -1.
     * @param key key whose index to search for
     * @return the index corresponding to that key -1.
     */
    int getKeyIndex(long key);

    /**
     * Splits the current node into 2 nodes.
     * Current node with all the keys from 0...at-1 and a new one from at+1...end.
     * @param at the key index that we are going to split by
     * @param splitNode the new node that will be populated with the second half of the split
     */
    void split(int at, BTreeNode splitNode);

    /**
     * Creates a copy of the node. The copy has the same id as the original.
     * @param copyNode the node that will be populated with the same content as this node
     */
    void copy(BTreeNode copyNode);

    /**
     * Commits this node to a storage.
     * @return the address offset where the node was stored
     */
    long commit(NodesManager nodesManager);

    boolean isDirty();

    boolean needRebalancing(int threshold);

    boolean isInternal();

    /**
     * Gets value at index.
     * @param index the index to get the value at
     * @return the value at index
     */
    long getValue(int index);

    /**
     * Gets the child at index
     * @param index the index for which to get a child
     * @return the child
     */
    BTreeNode getChildAt(int index);

    void insertChild(int index, long key, BTreeNode child);

    void setChild(int index, BTreeNode child);

    int getChildrenNumber();
}
