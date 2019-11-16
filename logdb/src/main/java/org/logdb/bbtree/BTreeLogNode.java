package org.logdb.bbtree;

public interface BTreeLogNode extends BTreeNode
{
    int getLogKeyValuesCount();

    int binarySearchInLog(long key);

    long getLogValue(int logIndex);

    void removeLogAtIndex(int index);
}
