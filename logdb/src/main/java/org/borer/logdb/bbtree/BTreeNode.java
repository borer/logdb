package org.borer.logdb.bbtree;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.storage.NodesManager;

public interface BTreeNode
{
    /**
     * unique id of this node.
      * @return id
     */
    long getId();

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
     * Splits the current node into 2 nodes.
     * Current node with all the keys from 0...at-1 and a new one from at+1...end.
     * @param at the key index that we are going to split by
     * @param memoryForNewNode the memory used for backing the content of the new node
     * @return a new node containing from at+1...end children
     */
    BTreeNode split(int at, Memory memoryForNewNode);

    /**
     * Creates a copy of the node. The copy has the same id as the original.
     * @param memoryForCopy memory gto backed the content of the copy node
     * @return a deep copy of the this node
     */
    BTreeNode copy(Memory memoryForCopy);

    /**
     * Commits this node to a storage.
     * @return the address offset where the node was stored
     */
    long commit(NodesManager nodesManager);

    boolean isDirty();

    boolean needRebalancing(int threshold);
}
