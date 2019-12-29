package org.logdb.bbtree;

import org.logdb.storage.PageNumber;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;

public interface BTree extends AutoCloseable
{
    void remove(byte[] key);

    void put(byte[] key, byte[] value);

    byte[] get(byte[] key, @Version long version);

    byte[] get(byte[] key);

    byte[] getByTimestamp(byte[] key, @Milliseconds long timestamp);

    void commit() throws IOException;

    String print();

    long getNodesCount();

    @PageNumber long getCommittedRoot();

    BTreeNode getUncommittedRoot();
}
