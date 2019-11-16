package org.logdb.bbtree;

public interface BTreeLogNode extends BTreeNode
{
    int getLogKeyValuesCount();

    int binarySearchInLog(long key);

    long getLogValue(int index);

    void removeLogAtIndex(int index);

    /**
     * try to remove a key/value pair for this node log.
     * @param key the key that identifies the key/value pair to remove from the node log
     */
    void removeLogWithKey(long key);
}
