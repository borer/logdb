package org.borer.logdb;

public interface BTreeNode
{
    /**
     * unique id of this node.
      * @return
     */
    long getId();

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
     * @return a new node containing from at+1...end children
     */
    BTreeNode split(int at);

    /**
     * Creates a copy of the node. The copy has the same id as the original.
     * @return a deep copy of the this node
     */
    BTreeNode copy();

    boolean needRebalancing(int threshold);
}
