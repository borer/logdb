package org.borer.logdb;

import java.nio.ByteBuffer;

public interface BTreeNode
{
    void insert(ByteBuffer key, ByteBuffer value);

    void remove(ByteBuffer key);

    int getKeyCount();

    ByteBuffer getKey(int index);

    ByteBuffer get(ByteBuffer key);

    BTreeNode split(int at);

    void print(StringBuilder printer, String label);
}
