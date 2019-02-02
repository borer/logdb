package org.borer.logdb;

import java.nio.ByteBuffer;

public interface BTreeNode
{
    /**
     * unique id for the node
      * @return
     */
    String getId();

    /**
     * Inserts key/value pair in the current leaf.
     * If the key already exits, its value is replaced.
     * @param key Key that identifies the value
     * @param value Value to persist
     */
    void insert(ByteBuffer key, ByteBuffer value);

    /**
     * Remove the key and value.
     *
     * @param key the key to remove
     */
    void remove(ByteBuffer key);

    /**
     * Get the number of entries in the leaf.
     * @return the number of entries.
     */
    int getKeyCount();

    ByteBuffer getKey(int index);

    BTreeNode getRightSibling();

    void setLeftSibling(BTreeNode leftSibling);

    void setRightSibling(BTreeNode rightSibling);

    BTreeNode getLeftSibling();

    /**
     * Get the value corresponding to the key.
     * @param key key to search for
     * @return the value corresponding to that key or null if not found
     */
    ByteBuffer get(ByteBuffer key);

    /**
     * Splits the current node into 2 nodes.
     * Current node with all the keys from 0...at-1 and a new one from at+1...end.
     * @param at the key index that we are going to split by
     * @return a new node containing from at+1...end children
     */
    BTreeNode split(int at);

    void print(StringBuilder printer);

    BTreeNode copy();
}
