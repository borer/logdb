package org.borer.logdb.bbtree;

public interface BTree
{
    void remove(long key);

    void put(long key, long value);

    long get(long key, int version);

    long get(long key);

    void commit();

    void close();

    String print();

    long getNodesCount();

    long getCommittedRoot();

    BTreeNode getUncommittedRoot();
}
