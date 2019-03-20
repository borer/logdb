package org.borer.logdb.storage;

import org.borer.logdb.BTreeNode;
import org.borer.logdb.BTreeNodeLeaf;
import org.borer.logdb.BTreeNodeNonLeaf;
import org.borer.logdb.IdSupplier;

import java.util.ArrayList;
import java.util.List;

public class NodesManager
{
    private final FileStorage fileStorage;
    private final IdSupplier idSupplier;

    private final List<BTreeNode> dirtyNodes;

    public NodesManager(FileStorage fileStorage)
    {
        this.fileStorage = fileStorage;
        this.idSupplier = new IdSupplier();
        this.dirtyNodes = new ArrayList<>();
    }

    public BTreeNodeLeaf createEmptyLeafNode()
    {
        final BTreeNodeLeaf leaf = new BTreeNodeLeaf(
                fileStorage.allocateWritableMemory(),
                0,
                0,
                idSupplier);

        dirtyNodes.add(leaf);

        return leaf;
    }

    public BTreeNodeNonLeaf createEmptyNonLeafNode()
    {
        final BTreeNodeNonLeaf nonLeaf = new BTreeNodeNonLeaf(
                fileStorage.allocateWritableMemory(),
                0,
                1,
                new BTreeNode[1],
                idSupplier);

        dirtyNodes.add(nonLeaf);

        return nonLeaf;
    }

    public BTreeNode splitNode(final BTreeNode originalNode, final int at)
    {
        final BTreeNode split = originalNode.split(at, fileStorage.allocateWritableMemory());

        dirtyNodes.add(split);

        return split;
    }

    public BTreeNode copyNode(final BTreeNode originalNode)
    {
        final BTreeNode copy = originalNode.copy(fileStorage.allocateWritableMemory());
        dirtyNodes.add(copy);

        return copy;
    }
}
