package org.logdb.bbtree;

import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;

public interface BTreeNode
{
    /**
     * The page number where this node is persisted or a generated id if it's not yet persisted.
      * @return pagenumber
     */
    @PageNumber long getPageNumber();

    /**
     * Inserts key/value pair in the current leaf.
     * If the key already exits, its value is replaced.
     * @param key Key that identifies the value
     * @param value Value to persist
     */
    void insert(byte[] key, byte[] value);

    /**
     * Remove the key and value at index.
     * @param index the index of the element to remove
     */
    void removeAtIndex(int index);

    /**
     * Get the number of entries in the leaf.
     * @return the number of entries.
     */
    int getPairCount();

    /**
     * Returns a key at the given index.
     * @param index has to be between 0...getKeyCount()
     * @return the key
     */
    byte[] getKey(int index);

    byte[] getMinKey();

    byte[] getMaxKey();

    /**
     * Get the value corresponding to the key.
     * @param key key to search for
     * @return the value corresponding to that key or null if not found
     */
    byte[] get(byte[] key);

    /**
     * Get the key index corresponding to this key or -1.
     * @param key key whose index to search for
     * @return the index corresponding to that key -1. It's >= 0 if found and  negative if not found
     */
    int getKeyIndex(byte[] key);

    /**
     * Splits the current node into 2 nodes.
     * Current node with all the keys from 0...at-1 and a new one from at+1...end.
     * @param at the key index that we are going to split by
     * @param splitNode the new node that will be populated with the second half of the split
     */
    void split(int at, BTreeNodeHeap splitNode);

    /**
     * Creates a copy of the node. The copy has the same id as the original.
     * @param destinationNode the node that will be populated with the same content as this node
     */
    void copy(BTreeNodeHeap destinationNode);

    /**
     * Commits this node to a storage.
     * @param nodesManager the node manager use for committing this node
     * @param isRoot specifies if the current node is root
     * @param previousRootPageNumber the page number of the previous root node. If first root, then this value is -1
     * @return the address offset where the node was stored
     */
    @PageNumber long commit(
            NodesManager nodesManager,
            boolean isRoot,
            @PageNumber long previousRootPageNumber,
            @Milliseconds long timestamp,
            @Version long version) throws IOException;

    boolean isDirty();

    BtreeNodeType getNodeType();

    /**
     * Gets value at index.
     * @param index the index to get the value at
     * @return the value at index
     */
    byte[] getValue(int index);

    /**
     * Gets the child at index.
     * @param index the index for which to get a child
     * @return the child
     */
    BTreeNode getChildAt(int index);

    void insertChild(int index, byte[] key, BTreeNodeHeap child);

    void setChild(int index, BTreeNodeHeap child);

    void setVersion(@Version long newVersion);

    @Version long getVersion();

    /**
     * Calculates if the node needs splitting. This only considers the node key/value paris and ignores the buffer size.
     * So it's possible for the node to be full because the log is full.
     * @param requiredSpace The space intended to store and should check if it's goign to fit before inserting
     * @return returns true if the node needs splitting, false other wise
     */
    boolean shouldSplit(@ByteSize int requiredSpace);

    void reset();
}
