package org.borer.logdb;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

public class BTree
{
    private static final int MAX_CHILDREN_PER_NODE = 10;
    /**
     * This designates the "last stored" version for a store which was
     * just open for the first time.
     */
    private static final long INITIAL_VERSION = 0;

    /**
     * Reference to the current root page.
     */
    private final AtomicReference<RootReference> root;

    public BTree(final BTreeNode root)
    {
        this.root = new AtomicReference<>(null);
        setNewRoot(null, root);
    }

    /**
     * Remove a key and the associated value, if the key exists.
     *
     * @param key the key (may not be null)
     */
    public void remove(final ByteBuffer key)
    {
        assert key != null;
        BTreeNode rootNode = getCurrentRootNode();
        final CursorPosition cursorPosition = traverseDown(rootNode, key);

        BTreeNode currentNode = cursorPosition.node;
        CursorPosition parentCursor = cursorPosition.parent;

        if (currentNode.getKeyCount() == 1 && parentCursor != null)
        {
            currentNode = parentCursor.node;
            parentCursor = parentCursor.parent;

            if (currentNode.getKeyCount() == 1)
            {
                int index = cursorPosition.index;
                assert index <= 1;
                final BTreeNodeNonLeaf currentNonLeafNode = (BTreeNodeNonLeaf)currentNode;
                currentNode = currentNonLeafNode.getChildPage(1 - index);

                updatePathToRoot(parentCursor, currentNode);
                return;
            }
            assert currentNode.getKeyCount() > 1;
        }

        currentNode = currentNode.copy();
        currentNode.remove(key);

        updatePathToRoot(parentCursor, currentNode);
    }

    /**
     * Inserts a new key/value into the BTree. Grows the tree if needed.
     * If the key already exists, overrides the value for it
     *
     * @param key   the kay to insert/set
     * @param value the value
     */
    public void put(final ByteBuffer key, final ByteBuffer value)
    {
        assert key != null;
        assert value != null;

        BTreeNode rootNode = getCurrentRootNode();
        final CursorPosition cursorPosition = traverseDown(rootNode, key);

        BTreeNode currentNode = cursorPosition.node;
        CursorPosition parentCursor = cursorPosition.parent;

        currentNode = currentNode.copy();
        currentNode.insert(key, value);

        int keyCount = currentNode.getKeyCount();
        while (keyCount > MAX_CHILDREN_PER_NODE)
        {
            final int at = keyCount >> 1;
            final ByteBuffer keyAt = currentNode.getKey(at);
            final BTreeNode split = currentNode.split(at);

            if (parentCursor == null)
            {
                final ByteBuffer[] keys = {keyAt};
                final BTreeNode[] children = {currentNode, split};
                currentNode = BTreeNodeAbstract.create(
                        keys,
                        null,
                        children,
                        null,
                        null);
                break;
            }

            final BTreeNodeNonLeaf parentNode = (BTreeNodeNonLeaf) parentCursor.node.copy();
            parentNode.setChild(parentCursor.index, split);
            parentNode.insertChild(parentCursor.index, keyAt, currentNode);

            parentCursor = parentCursor.parent;
            currentNode = parentNode;
            keyCount = currentNode.getKeyCount();
        }

        updatePathToRoot(parentCursor, currentNode);
    }

    /**
     * Gets a value for the key at time/instance t
     * @param key the key to search for
     * @param version the version that we are interested. Must be >= 0
     */
    public ByteBuffer get(final ByteBuffer key, final int version)
    {
        assert key != null;
        assert version >= 0;

        RootReference rootReference = getRootReferenceForVersion(version);
        if (rootReference == null)
        {
            return null;
        }

        final BTreeNode rootNode = rootReference.root;
        return rootNode.get(key);
    }

    public ByteBuffer get(final ByteBuffer key)
    {
        assert key != null;

        final BTreeNode rootNode = getCurrentRootNode();
        return rootNode.get(key);
    }

    /**
     * Outputs graphivz format that represents the B+tree
     * @param printer Buffer used for output
     */
    public void print(StringBuilder printer)
    {
        printer.append("digraph g {\n");
        printer.append("node [shape = record,height=.1];\n");

        getCurrentRootNode().print(printer);

        printer.append("}\n");
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
            index = SearchUtils.binarySearch(key, nonLeaf.keys) + 1;
            if (index < 0)
            {
                index = -index;
            }
            cursor = new CursorPosition(node, index, cursor);
            node = nonLeaf.getChildPage(index);
        }

        return new CursorPosition(node, index, cursor);
    }

    private void updatePathToRoot(final CursorPosition cursor, final BTreeNode current)
    {
        BTreeNode currentNode = current;
        CursorPosition parentCursor = cursor;

        while (parentCursor != null)
        {
            BTreeNode c = currentNode;
            currentNode = parentCursor.node.copy();
            ((BTreeNodeNonLeaf)currentNode).setChild(parentCursor.index, c);
            parentCursor = parentCursor.parent;
        }

        setNewRoot(getCurrentRoot(), currentNode);
    }

    private RootReference getRootReferenceForVersion(int version)
    {
        RootReference rootReference = getCurrentRoot();
        while (rootReference != null && rootReference.version > version)
        {
            rootReference = rootReference.previous;
        }

        if (rootReference == null || rootReference.version < version)
        {
            return null;
        }
        return rootReference;
    }

    private RootReference getCurrentRoot()
    {
        return root.get();
    }

    private BTreeNode getCurrentRootNode()
    {
        RootReference rootReference = root.get();
        assert rootReference != null;
        return rootReference.root;
    }

    /**
     * Try to set the new root reference from now on.
     *
     * @param oldRoot previous root reference
     * @param newRootPage the new root page
     * @return new RootReference or null if update failed
     */
    private RootReference setNewRoot(RootReference oldRoot, BTreeNode newRootPage)
    {
        RootReference currentRoot = getCurrentRoot();
        assert newRootPage != null || currentRoot != null;
        if (currentRoot != oldRoot && oldRoot != null)
        {
            return null;
        }

        long newVersion = INITIAL_VERSION;
        if (currentRoot != null)
        {
            if (newRootPage == null)
            {
                newRootPage = currentRoot.root;
            }

            newVersion = currentRoot.version + 1L;
        }

        RootReference updatedRootReference = new RootReference(newRootPage, newVersion, currentRoot);
        boolean success = root.compareAndSet(currentRoot, updatedRootReference);
        return success ? updatedRootReference : null;
    }

    public static final class RootReference
    {
        /**
         * The root page.
         */
        public final BTreeNode root;
        /**
         * The version used for writing.
         */
        public final long version;
        /**
         * Reference to the previous root in the chain.
         */
        public volatile RootReference previous;

        RootReference(BTreeNode root, long version, RootReference previous)
        {
            this.root = root;
            this.version = version;
            this.previous = previous;
        }
    }
}
