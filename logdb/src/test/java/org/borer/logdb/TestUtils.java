package org.borer.logdb;

import org.borer.logdb.bit.MemoryFactory;

import java.nio.ByteBuffer;

import static org.borer.logdb.Config.BYTE_ORDER;
import static org.borer.logdb.Config.PAGE_SIZE_BYTES;

class TestUtils
{
    static ByteBuffer createValue(final String value)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(value.length());
        buffer.put(value.getBytes());
        buffer.rewind();

        return buffer;
    }

    static BTreeNodeNonLeaf createNonLeafNodeWithChild(final BTreeNode child)
    {
        final BTreeNodeNonLeaf nonLeaf = new BTreeNodeNonLeaf(
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                1, //there is always one child at least
                new BTreeNode[1],
                new IdSupplier());

        nonLeaf.setChild(0, child);

        return nonLeaf;
    }

    static BTreeNodeLeaf createLeafNodeWithKeys(final int numKeys, final int startKey)
    {
        final BTreeNodeLeaf bTreeNode = new BTreeNodeLeaf(
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                0,
                new IdSupplier(startKey));
        for (int i = 0; i < numKeys; i++)
        {
            final int key = startKey + i;
            bTreeNode.insert(key, key);
        }

        return bTreeNode;
    }
}