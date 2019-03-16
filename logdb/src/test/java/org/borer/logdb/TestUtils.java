package org.borer.logdb;

import java.nio.ByteBuffer;

class TestUtils
{
    static ByteBuffer createValue(final String value)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(value.length());
        buffer.put(value.getBytes());
        buffer.rewind();

        return buffer;
    }

    static BTreeNodeLeaf createLeafNodeWithKeys(final int numKeys, final int startKey)
    {
        final BTreeNodeLeaf bTreeNode = new BTreeNodeLeaf(startKey);
        for (int i = 0; i < numKeys; i++)
        {
            final int key = startKey + i;
            bTreeNode.insert(key, key);
        }

        return bTreeNode;
    }
}