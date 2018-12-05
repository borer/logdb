package org.borer.logdb;

import java.nio.ByteBuffer;

public class BTree
{
    private static final int MAX_CHILDREN_PER_NODE = 10;

    private BTreeNode root;

    public BTree(final BTreeNode root)
    {
        this.root = root;
    }

    /**
     * Inserts a new key/value into the BTree. Grows the tree if needed.
     * If the key already exists, overrides the value for it
     * @param key the kay to insert/set
     * @param value the value
     */
    public void insert(final ByteBuffer key, final ByteBuffer value)
    {
        final CursorPosition cursorPosition = traverseDown(root, key);

        BTreeNode currentNode = cursorPosition.node;
        CursorPosition parentCursor = cursorPosition.parent;
        currentNode.insert(key, value);

        int keyCount = currentNode.getKeyCount();
        while (keyCount > MAX_CHILDREN_PER_NODE)
        {
            final int at = keyCount >> 1;
            final ByteBuffer keyAt = currentNode.getKey(at);
            final BTreeNode split = currentNode.split(at);

            if (parentCursor == null)
            {
                final ByteBuffer[] keys = { keyAt };
                final BTreeNode[] children = { currentNode, split };
                root = BTreeNodeAbstract.create(keys, null, children);
                break;
            }

            final BTreeNodeNonLeaf parentNode = (BTreeNodeNonLeaf) parentCursor.node;
            parentNode.setChild(parentCursor.index, split);
            parentNode.insertChild(parentCursor.index, keyAt, currentNode);

            parentCursor = parentCursor.parent;
            currentNode = parentNode;
            keyCount = currentNode.getKeyCount();
        }
    }

    public void print(StringBuilder printer)
    {
        root.print(printer, "root");
    }

    private static CursorPosition traverseDown(final BTreeNode root, final ByteBuffer key)
    {
        BTreeNode node = root;
        CursorPosition cursor = null;
        int index = -1;
        while (node instanceof BTreeNodeNonLeaf)
        {
            final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;
            assert nonLeaf.getKeyCount() > 0;
            index = nonLeaf.binarySearch(key) + 1;
            if (index < 0)
            {
                index = -index;
            }
            cursor = new CursorPosition(node, index, cursor);
            node = nonLeaf.getChildPage(index);
        }

        return new CursorPosition(node, index, cursor);
    }
}
