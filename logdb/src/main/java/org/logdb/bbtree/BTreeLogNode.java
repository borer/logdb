package org.logdb.bbtree;

public interface BTreeLogNode extends BTreeNode
{
    int getLogKeyValuesCount();

    boolean hasKeyLog(byte[] key);

    byte[] getLogValue(byte[] key);

    byte[] getLogValueAtIndex(int index);

    /**
     * try to remove a key/value pair for this node log.
     * @param key the key that identifies the key/value pair to remove from the node log
     */
    void removeLog(byte[] key);
}
