package org.logdb.bbtree;

import org.logdb.storage.PageNumber;

final class CursorPosition
{
    private final BTreeNode node;
    private final @PageNumber long pageNumber;

    final int index;
    final CursorPosition parent;

    /**
     *  Stores a location when inserting/searching in the btree.
     * @param node the btree node if any. Could be null
     * @param pageNumber the page number for the node, if node is not present. Otherwise -1
     * @param index the index inside that node
     * @param parent the parent of the current node
     */
    CursorPosition(final BTreeNode node, final @PageNumber long pageNumber, final int index, final CursorPosition parent)
    {
        this.node = node;
        this.pageNumber = pageNumber;
        this.index = index;
        this.parent = parent;
    }

    private boolean isMapped()
    {
        return node == null && pageNumber > 0;
    }

    BTreeNode getNode(final BTreeMappedNode mappedNode)
    {
        if (isMapped())
        {
            mappedNode.initNode(pageNumber);
            return mappedNode;
        }
        else
        {
            return node;
        }
    }
}
