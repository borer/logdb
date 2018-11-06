package org.borer.logdb;

import java.nio.ByteBuffer;

public interface BTree
{
    void insert(ByteBuffer key, ByteBuffer value);

    void remove(ByteBuffer key);

    ByteBuffer get(ByteBuffer key);

    BTree split(int at);
}
