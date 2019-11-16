package org.logdb.bbtree;

public interface BTreeLogNode extends BTreeNode
{
    int getLogKeyValuesCount();

    boolean hasKeyLog(long key);

    long getLogValue(long key);

    long getLogValueAtIndex(int index);

    /**
     * try to remove a key/value pair for this node log.
     * @param key the key that identifies the key/value pair to remove from the node log
     */
    void removeLog(long key);

    void removeLogAtIndex(int index);
}
