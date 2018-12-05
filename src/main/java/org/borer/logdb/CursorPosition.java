package org.borer.logdb;

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
}
