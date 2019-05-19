package org.logdb.bbtree;

import org.logdb.storage.NodesManager;

public interface BTreeNode
{
    /**
     * The page number where this node is persisted or a generated id if it's not yet persisted.
      * @return pagenumber
     */
    long getPageNumber();

    /**
     * Reload node in memory data from backing storage.
     */
    void initNodeFromBuffer();

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

    long getMinKey();

    long getMaxKey();

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
    void split(int at, BTreeNodeHeap splitNode);

    /**
     * Creates a copy of the node. The copy has the same id as the original.
     * @param copyNode the node that will be populated with the same content as this node
     */
    void copy(BTreeNodeHeap copyNode);

    /**
     * Commits this node to a storage.
     * @param nodesManager the node manager use for committing this node
     * @param isRoot specifies if the current node is root
     * @param previousRootPageNumber the page number of the previous root node. If first root, then this value is -1
     * @return the address offset where the node was stored
     */
    long commit(NodesManager nodesManager, boolean isRoot, long previousRootPageNumber, long timestamp, long version);

    boolean isDirty();

    BtreeNodeType getNodeType();

    /**
     * Gets value at index.
     * @param index the index to get the value at
     * @return the value at index
     */
    long getValue(int index);

    /**
     * Gets the child at index.
     * @param index the index for which to get a child
     * @return the child
     */
    BTreeNode getChildAt(int index);

    void insertChild(int index, long key, BTreeNodeHeap child);

    void setChild(int index, BTreeNodeHeap child);

    int getChildrenNumber();

    void setVersion(long newVersion);

    long getVersion();

    /**
     * Calculates if the node needs splitting. This only considers the node key/value paris and ignores the buffer size.
     * So it's possible for the node to be full because the log is full.
     * @return returns true if the node needs splitting, false other wise
     */
    boolean shouldSplit();

    void reset();
}
