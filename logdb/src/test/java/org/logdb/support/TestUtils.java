package org.logdb.support;

import org.logdb.bbtree.BTreeNodeHeap;
import org.logdb.bbtree.BTreeNodeLeaf;
import org.logdb.bbtree.BTreeNodeNonLeaf;
import org.logdb.bbtree.IdSupplier;
import org.logdb.bbtree.NodesManager;
import org.logdb.bbtree.RootReference;
import org.logdb.bit.MemoryFactory;
import org.logdb.root.index.RootIndex;
import org.logdb.storage.ByteSize;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.storage.memory.MemoryStorage;
import org.logdb.time.TimeUnits;

import java.nio.ByteOrder;

public class TestUtils
{
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    public static final int PAGE_SIZE_BYTES = 512;
    public static final int NODE_LOG_SIZE = 154;
    public static final @ByteSize int ZERO_NODE_LOG_SIZE = StorageUnits.ZERO_SIZE;

    public static final long SEGMENT_FILE_SIZE = PAGE_SIZE_BYTES * 200;
    public static final @ByteSize int MEMORY_CHUNK_SIZE = StorageUnits.size(PAGE_SIZE_BYTES * 200);
    public static final @Version long INITIAL_VERSION = StorageUnits.version(0);

    public static RootIndex createRootIndex(final @ByteSize int pageSize)
    {
        final Storage rootIndexStorage = new MemoryStorage(TestUtils.BYTE_ORDER, pageSize, MEMORY_CHUNK_SIZE);
        return new RootIndex(
                rootIndexStorage,
                StorageUnits.INITIAL_VERSION,
                TimeUnits.millis(0L),
                StorageUnits.ZERO_OFFSET);
    }

    public static RootReference createInitialRootReference(final NodesManager nodesManager)
    {
        return new RootReference(
                nodesManager.createEmptyLeafNode(),
                TimeUnits.millis(0),
                StorageUnits.INITIAL_VERSION,
                null);
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
                NODE_LOG_SIZE,
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
                0);
        for (int i = 0; i < numKeys; i++)
        {
            final int key = startKey + i;
            bTreeNode.insert(key, key);
        }

        return bTreeNode;
    }
}