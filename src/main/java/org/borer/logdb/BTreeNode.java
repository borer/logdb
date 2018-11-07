package org.borer.logdb;

import java.nio.ByteBuffer;

public interface BTreeNode
{
    void insert(ByteBuffer key, ByteBuffer value);

    void insertChild(ByteBuffer key, BTreeNode child);

    void remove(ByteBuffer key);

    ByteBuffer get(ByteBuffer key);

    BTreeNode split(int at);
}
