package org.logdb.bbtree;

import org.logdb.storage.Version;

public interface BTree
{
    void remove(long key);

    void put(long key, long value);

    long get(long key, @Version long version);

    long get(long key);

    void commit();

    void close();

    String print();

    long getNodesCount();

    long getCommittedRoot();

    BTreeNode getUncommittedRoot();
}
