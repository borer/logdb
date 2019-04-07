package org.borer.logdb.storage;

import org.borer.logdb.bbtree.BTreeMappedNode;
import org.borer.logdb.bbtree.BTreeNode;
import org.borer.logdb.bbtree.BTreeNodeHeap;
import org.borer.logdb.bbtree.BTreeNodeLeaf;
import org.borer.logdb.bbtree.BTreeNodeNonLeaf;
import org.borer.logdb.bbtree.BtreeNodeType;
import org.borer.logdb.bbtree.IdSupplier;
import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.ReadMemory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

public class NodesManager
{
    private final Storage storage;
    private final IdSupplier idSupplier;

    private final List<BTreeNode> dirtyRootNodes;
    //TODO: use an LRU bounded cache
    private final Map<Long, BTreeNodeHeap> nonLeafNodesCache;
    private final Collection<BTreeNodeHeap> leafNodesCache;

    private final Queue<BTreeMappedNode> mappedNodes;

    public NodesManager(final Storage storage)
    {
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.idSupplier = new IdSupplier();
        this.dirtyRootNodes = new ArrayList<>();
        this.nonLeafNodesCache = new HashMap<>();
        leafNodesCache = new ArrayList<>();
        mappedNodes = new ArrayDeque<>();
    }

    private BTreeNodeLeaf createEmptyLeafNode()
    {
        return new BTreeNodeLeaf(
                idSupplier.getAsLong(),
                storage.allocateHeapMemory(),
                0,
                0);
    }

    public BTreeMappedNode getMappedNode()
    {
        if (!mappedNodes.isEmpty())
        {
            return mappedNodes.poll();
        }
        else
        {
            return new BTreeMappedNode(
                    storage,
                    storage.getDirectMemory(0),
                    storage.getPageSize(),
                    0);
        }
    }

    public void returnMappedNode(final BTreeMappedNode mappedNode)
    {
        mappedNodes.add(mappedNode);
    }

    public BTreeNodeNonLeaf createEmptyNonLeafNode()
    {
        return new BTreeNodeNonLeaf(
                idSupplier.getAsLong(),
                storage.allocateHeapMemory(),
                0,
                1,
                new BTreeNode[1]);
    }

    public BTreeNodeHeap splitNode(final BTreeNode originalNode, final int at)
    {
        final BTreeNodeHeap splitNode = createSameNodeType(originalNode, storage.allocateHeapMemory());
        originalNode.split(at, splitNode);
        return splitNode;
    }

    public BTreeNodeHeap copyNode(final BTreeNode originalNode)
    {
        final BTreeNodeHeap copyNode = createSameNodeType(originalNode, storage.allocateHeapMemory());
        originalNode.copy(copyNode);
        return copyNode;
    }

    private BTreeNodeHeap createSameNodeType(final BTreeNode originalNode, final Memory memory)
    {
        return (originalNode.isInternal())
                ? new BTreeNodeNonLeaf(idSupplier.getAsLong(), memory, 0, 0, null)
                : new BTreeNodeLeaf(idSupplier.getAsLong(), memory, 0, 0);
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

    public long commitNode(final BTreeNodeHeap node)
    {
        final ReadMemory buffer = node.getBuffer();
        final long pageNumber = storage.commitNode(buffer);
        if (node.isInternal())
        {
            nonLeafNodesCache.put(pageNumber, node);
        }
        else
        {
            leafNodesCache.add(node);
        }

        return pageNumber;
    }

    public BTreeNode loadNode(final int index, final BTreeNode parentNode, final BTreeMappedNode mappedNode)
    {
        assert parentNode.isInternal() : "node must be internal";

        final long childPageNumber = parentNode.getValue(index);
        if (childPageNumber != BTreeNodeNonLeaf.NON_COMMITTED_CHILD)
        {
            mappedNode.initNode(childPageNumber);
            return mappedNode;
        }
        else
        {
            return parentNode.getChildAt(index);
        }
    }

    public void commitLastRootPage(final long offsetLastRoot)
    {
        storage.commitMetadata(offsetLastRoot);
    }

    public BTreeNode loadLastRoot()
    {
        final Memory memory = (Memory) storage.loadLastRoot();
        final long pageNumber = storage.getLastRootPageNumber();

        if (memory != null)
        {
            final BtreeNodeType nodeType = BtreeNodeType.fromByte(memory.getByte(0));

            if (BtreeNodeType.Leaf == nodeType)
            {
                //TODO: create a pool of those
                return new BTreeNodeLeaf(pageNumber, memory);
            }
            else
            {
                final BTreeNodeNonLeaf bTreeNodeNonLeaf = new BTreeNodeNonLeaf(pageNumber, memory);
                nonLeafNodesCache.put(pageNumber, bTreeNodeNonLeaf);
                return bTreeNodeNonLeaf;
            }
        }
        else //Condition used when creating new btree
        {
            return createEmptyLeafNode();
        }
    }

    public void close()
    {
        try
        {
            mappedNodes.clear();
            storage.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
