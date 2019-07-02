package org.logdb.bbtree;

import org.logdb.storage.PageNumber;
import org.logdb.storage.Version;

import java.io.IOException;

public interface BTree extends AutoCloseable
{
    void remove(long key);

    void put(long key, long value);

    long get(long key, @Version long version);

    long get(long key);

    void commit() throws IOException;

    String print();

    long getNodesCount();

    @PageNumber long getCommittedRoot();

    BTreeNode getUncommittedRoot();
}
