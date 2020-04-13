package org.logdb.bbtree;

import org.logdb.Index;
import org.logdb.storage.PageNumber;
import org.logdb.time.Milliseconds;

public interface BTree extends Index
{
    byte[] getByTimestamp(byte[] key, @Milliseconds long timestamp);

    String print();

    long getNodesCount();

    @PageNumber long getCommittedRoot();

    BTreeNode getUncommittedRoot();
}
