package org.borer.logdb.storage;

import org.borer.logdb.bbtree.BTreeNode;
import org.borer.logdb.bbtree.BTreeNodeLeaf;
import org.borer.logdb.bbtree.BTreeNodeNonLeaf;
import org.borer.logdb.bbtree.BtreeNodeType;
import org.borer.logdb.bbtree.IdSupplier;
import org.borer.logdb.bit.Memory;

import java.util.ArrayList;
import java.util.List;

public class NodesManager
{
    private final Storage storage;
    private final IdSupplier idSupplier;

    private final List<BTreeNode> dirtyRootNodes;

    public NodesManager(final Storage storage)
    {
        this.storage = storage;
        this.idSupplier = new IdSupplier();
        this.dirtyRootNodes = new ArrayList<>();
    }

    public BTreeNodeLeaf createEmptyLeafNode()
    {
        final BTreeNodeLeaf leaf = new BTreeNodeLeaf(
                storage.allocateWritableMemory(),
                0,
                0,
                idSupplier);

        return leaf;
    }

    public BTreeNodeNonLeaf createEmptyNonLeafNode()
    {
        final BTreeNodeNonLeaf nonLeaf = new BTreeNodeNonLeaf(
                storage.allocateWritableMemory(),
                0,
                1,
                new BTreeNode[1],
                idSupplier);

        return nonLeaf;
    }

    public BTreeNode splitNode(final BTreeNode originalNode, final int at)
    {
        final BTreeNode split = originalNode.split(at, storage.allocateWritableMemory());
        return split;
    }

    public BTreeNode copyNode(final BTreeNode originalNode)
    {
        final BTreeNode copy = originalNode.copy(storage.allocateWritableMemory());
        return copy;
    }

    public void addDirtyRoot(final BTreeNode rootNode)
    {
        dirtyRootNodes.add(rootNode);
    }

    public void commitDirtyNodes()
    {
        for (final BTreeNode dirtyRootNode : dirtyRootNodes)
        {
            //TODO: After commit, change node memory to direct mapped and return the heap memory
            dirtyRootNode.commit(storage);
        }

        storage.flush();

        //TODO: maybe extend storage mapped areas.

        dirtyRootNodes.clear();
    }

    public BTreeNode loadNode(final long pageNumber)
    {
        //TODO: load from mapped buffers this node
        return null;
    }

    public void commitLastRoot(final long offsetLastRoot)
    {
        storage.commitMetadata(offsetLastRoot);
    }

    public BTreeNode loadLastRoot()
    {
        final Memory memory = storage.loadLastRoot();
        if (memory == null)
        {
            return createEmptyLeafNode();
        }
        else
        {
            final BtreeNodeType nodeType = BtreeNodeType.fromByte(memory.getByte(0));

            if (BtreeNodeType.Leaf == nodeType)
            {
                return new BTreeNodeLeaf(memory, idSupplier);
            }
            else
            {
                return new BTreeNodeNonLeaf(memory, idSupplier);
            }
        }
    }
}
