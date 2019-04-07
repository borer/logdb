package org.borer.logdb.bbtree;

import org.borer.logdb.storage.NodesManager;

public final class CursorPosition
{
    final BTreeNode node;
    final int index;
    final CursorPosition parent;

    /**
     *  Stores a location when inserting/searching in the btree.
     * @param node the btree node
     * @param index the index inside that node
     * @param parent the parent of the current node
     */
    public CursorPosition(final BTreeNode node, final int index, final CursorPosition parent)
    {
        this.node = node;
        this.index = index;
        this.parent = parent;
    }

    public CursorPosition getRightSibling(final NodesManager nodesManager)
    {
        if (parent == null || parent.node == null)
        {
            return null;
        }

        //find common ancestor that has a right child
        CursorPosition parentCursor = this.parent;
        int siblingIndex = index + 1;
        BTreeNodeNonLeaf parentNode;
        do
        {
            parentNode = (BTreeNodeNonLeaf) parentCursor.node;
            if (siblingIndex > parentNode.getChildrenNumber())
            {
                break;
            }

            siblingIndex = parentCursor.index + 1;
            parentCursor = parentCursor.parent;
        }
        while (parentCursor != null);


        //traverse down the leftest child
        BTreeNode rightSiblingNode = nodesManager.loadNode(siblingIndex, parentNode);
        CursorPosition siblingCursorPosition = new CursorPosition(
                rightSiblingNode,
                siblingIndex,
                parentCursor);

        if (rightSiblingNode instanceof BTreeNodeLeaf)
        {
            return siblingCursorPosition;
        }
        else
        {
            return buildLeftestPathToLeaf(siblingCursorPosition, nodesManager);
        }
    }

    private CursorPosition buildLeftestPathToLeaf(final CursorPosition cursorPosition, final NodesManager nodesManager)
    {
        CursorPosition parentCursor = cursorPosition;
        BTreeNode node = cursorPosition.node;
        while (node instanceof BTreeNodeNonLeaf)
        {
            final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;
            parentCursor = new CursorPosition(nonLeaf, 0, parentCursor);

            node = nodesManager.loadNode(0, nonLeaf);
        }

        return new CursorPosition(node, 0, parentCursor);
    }

    public CursorPosition getLeftSibling(final NodesManager nodesManager)
    {
        if (parent == null || parent.node == null)
        {
            return null;
        }

        //find common ancestor that has a right child
        CursorPosition parentCursor = this.parent;
        int siblingIndex = index - 1;
        BTreeNodeNonLeaf parentNode;
        do
        {
            parentNode = (BTreeNodeNonLeaf) parentCursor.node;
            if (siblingIndex < 0 || siblingIndex > parentNode.getChildrenNumber())
            {
                break;
            }

            siblingIndex = parentCursor.index - 1;
            parentCursor = parentCursor.parent;
        }
        while (parentCursor != null);


        //traverse down the rightest child
        BTreeNode leftSiblingNode = nodesManager.loadNode(siblingIndex, parentNode);
        CursorPosition siblingCursorPosition = new CursorPosition(
                leftSiblingNode,
                siblingIndex,
                parentCursor);

        if (leftSiblingNode instanceof BTreeNodeLeaf)
        {
            return siblingCursorPosition;
        }
        else
        {
            return buildRightestPathToLeaf(siblingCursorPosition, nodesManager);
        }
    }

    private CursorPosition buildRightestPathToLeaf(final CursorPosition cursorPosition, final NodesManager nodesManager)
    {
        CursorPosition parentCursor = cursorPosition;
        BTreeNode node = cursorPosition.node;
        int parentIndex = cursorPosition.index;
        while (node instanceof BTreeNodeNonLeaf)
        {
            final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;

            parentCursor = new CursorPosition(nonLeaf, parentIndex, parentCursor);

            parentIndex = nonLeaf.getChildrenNumber();
            node = nodesManager.loadNode(parentIndex, nonLeaf);
        }

        return new CursorPosition(node, parentIndex, parentCursor);
    }
}
