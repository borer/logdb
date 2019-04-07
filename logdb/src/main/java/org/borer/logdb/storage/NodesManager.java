package org.borer.logdb.storage;

import org.borer.logdb.bbtree.BTreeNode;
import org.borer.logdb.bbtree.BTreeNodeLeaf;
import org.borer.logdb.bbtree.BTreeNodeNonLeaf;
import org.borer.logdb.bbtree.BtreeNodeType;
import org.borer.logdb.bbtree.IdSupplier;
import org.borer.logdb.bit.Memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class NodesManager
{
    private final Storage storage;
    private final IdSupplier idSupplier;

    private final List<BTreeNode> dirtyRootNodes;
    //TODO: use an LRU bounded cache
    private final Map<Long, BTreeNodeNonLeaf> nonLeafNodesCache;

    public NodesManager(final Storage storage)
    {
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.idSupplier = new IdSupplier();
        this.dirtyRootNodes = new ArrayList<>();
        this.nonLeafNodesCache = new HashMap<>();
    }

    private BTreeNodeLeaf createEmptyLeafNode()
    {
        return new BTreeNodeLeaf(
                idSupplier.getAsLong(),
                storage.allocateWritableMemory(),
                0,
                0);
    }

    public BTreeNodeNonLeaf createEmptyNonLeafNode()
    {
        return new BTreeNodeNonLeaf(
                idSupplier.getAsLong(),
                storage.allocateWritableMemory(),
                0,
                1,
                new BTreeNode[1]);
    }

    public BTreeNode splitNode(final BTreeNode originalNode, final int at)
    {
        final BTreeNode splitNode = createSameNodeType(originalNode, storage.allocateWritableMemory());
        originalNode.split(at, splitNode);
        return splitNode;
    }

    public BTreeNode copyNode(final BTreeNode originalNode)
    {
        final BTreeNode copyNode = createSameNodeType(originalNode, storage.allocateWritableMemory());
        originalNode.copy(copyNode);
        return copyNode;
    }

    private BTreeNode createSameNodeType(final BTreeNode originalNode, final Memory memory)
    {
        return (originalNode instanceof BTreeNodeLeaf)
                ? new BTreeNodeLeaf(idSupplier.getAsLong(), memory, 0, 0)
                : new BTreeNodeNonLeaf(idSupplier.getAsLong(), memory, 0, 0, null);
    }

    public void addDirtyRoot(final BTreeNode rootNode)
    {
        dirtyRootNodes.add(rootNode);
    }

    public void commitDirtyNodes()
    {
        int i = 0;
        for (final BTreeNode dirtyRootNode : dirtyRootNodes)
        {
            dirtyRootNode.commit(this);
            i++;
        }

        storage.flush();
        dirtyRootNodes.clear();
    }

    public long commitNode(final BTreeNode node)
    {
        final Memory buffer = node.getBuffer();
        final long pageNumber = storage.commitNode(buffer);
        storage.returnWritableMemory(buffer);

        node.updateBuffer(storage.loadPage(pageNumber));
        return pageNumber;
    }

    public BTreeNode loadNode(final int index, final BTreeNodeNonLeaf node)
    {
        final long childPageNumber = node.getValue(index);
        if (childPageNumber != BTreeNodeNonLeaf.NON_COMMITTED_CHILD)
        {
            return loadNode(childPageNumber);
        }
        else
        {
            return node.getChildAt(index);
        }
    }

    public BTreeNode loadNode(final long pageNumber)
    {
        if (nonLeafNodesCache.containsKey(pageNumber))
        {
            return nonLeafNodesCache.get(pageNumber);
        }
        else
        {
            final Memory nodeMemory = storage.loadPage(pageNumber);
            return constructBTreeNode(nodeMemory, pageNumber);
        }
    }

    public void commitLastRootPage(final long offsetLastRoot)
    {
        storage.commitMetadata(offsetLastRoot);
    }

    public BTreeNode loadLastRoot()
    {
        final Memory memory = storage.loadLastRoot();
        return constructBTreeNode(memory, storage.getLastRootPageNumber());
    }

    /**
     * Wraps the memory buffer in the appropriate node.
     * @param memory the memory buffer to wrap. It's the backing storage and content of the btree node
     * @param pageNumber the page number of that memory buffer
     * @return btree node wrapping the content of the memory buffer
     */
    private BTreeNode constructBTreeNode(final Memory memory, final long pageNumber)
    {
        if (memory != null)
        {
            final BtreeNodeType nodeType = BtreeNodeType.fromByte(memory.getByte(0));

            if (BtreeNodeType.Leaf == nodeType)
            {
                //TODO: create a pool of those
                return new BTreeNodeLeaf(idSupplier.getAsLong(), memory);
            }
            else
            {
                final BTreeNodeNonLeaf bTreeNodeNonLeaf = new BTreeNodeNonLeaf(idSupplier.getAsLong(), memory);
                nonLeafNodesCache.put(pageNumber, bTreeNodeNonLeaf);
                return bTreeNodeNonLeaf;
            }
        }
        else //Condition used when creating new btree
        {
            return createEmptyLeafNode();
        }
    }
}
