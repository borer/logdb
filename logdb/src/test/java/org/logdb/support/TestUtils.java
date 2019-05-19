package org.logdb.support;

import org.logdb.bbtree.BTreeNodeHeap;
import org.logdb.bbtree.BTreeNodeLeaf;
import org.logdb.bbtree.BTreeNodeNonLeaf;
import org.logdb.bbtree.IdSupplier;
import org.logdb.bit.MemoryFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestUtils
{
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    public static final int PAGE_SIZE_BYTES = 512;

    public static final long MAPPED_CHUNK_SIZE = PAGE_SIZE_BYTES * 200;

    public static ByteBuffer createValue(final String value)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(value.length());
        buffer.put(value.getBytes());
        buffer.rewind();

        return buffer;
    }

    public static BTreeNodeNonLeaf createNonLeafNodeWithChild(final BTreeNodeHeap child)
    {
        return createNonLeafNodeWithChild(child, 0);
    }

    public static BTreeNodeNonLeaf createNonLeafNodeWithChild(final BTreeNodeHeap child, final int startId)
    {
        final BTreeNodeNonLeaf nonLeaf = new BTreeNodeNonLeaf(
                startId,
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                0,
                1, //there is always one child at least
                new BTreeNodeHeap[1]);

        nonLeaf.setChild(0, child);

        return nonLeaf;
    }

    public static BTreeNodeLeaf createLeafNodeWithKeys(final int numKeys, final int startKey, IdSupplier idSupplier)
    {
        final BTreeNodeLeaf bTreeNode = new BTreeNodeLeaf(
                idSupplier.getAsLong(),
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                0,
                0);
        for (int i = 0; i < numKeys; i++)
        {
            final int key = startKey + i;
            bTreeNode.insert(key, key);
        }

        return bTreeNode;
    }
}