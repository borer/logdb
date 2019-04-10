package org.borer.logdb.bbtree;

import org.borer.logdb.Config;
import org.borer.logdb.storage.NodesManager;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class BTree
{
    private static final int THRESHOLD_CHILDREN_PER_NODE = 4;
    /**
     * This designates the "last stored" version for a store which was
     * just open for the first time.
     */
    private static final long INITIAL_VERSION = 0;

    /**
     * Reference to the current root page.
     */
    private final AtomicReference<RootReference> root;

    private final NodesManager nodesManager;

    private long nodesCount;

    public BTree(final NodesManager nodesManager)
    {
        this.nodesManager = Objects.requireNonNull(
                nodesManager, "nodesManager must not be null");
        this.root = new AtomicReference<>(null);
        this.nodesCount = 1;

        //TODO: pass the root in the constructor
        final BTreeNode currentRoot = Objects.requireNonNull(
                nodesManager.loadLastRoot(), "current root cannot be null");
        setNewRoot(null, currentRoot);
    }

    /**
     * Remove a key and the associated value, if the key exists.
     *
     * @param key the key (may not be null)
     */
    public void remove(final long key)
    {
        BTreeNode rootNode = getCurrentRootNode();
        final CursorPosition cursorPosition = traverseDown(rootNode, key);

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

        int index = cursorPosition.index;
        BTreeNode currentNode = cursorPosition.getNode(mappedNode);
        CursorPosition parentCursor = cursorPosition.parent;

        if (currentNode.getKeyCount() == 1 && parentCursor != null)
        {
            this.nodesCount--;
            index = parentCursor.index;
            currentNode = parentCursor.getNode(mappedNode);
            parentCursor = parentCursor.parent;

            if (currentNode.getKeyCount() == 1)
            {
                assert currentNode.isInternal()
                        : "Parent of the node that trying to remove is not non leaf";

                this.nodesCount--;
                assert index <= 1;
                currentNode = nodesManager.loadNode(1 - index, currentNode, mappedNode);

                final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode);
                updatePathToRoot(parentCursor, targetNode);
                return;
            }
            assert currentNode.getKeyCount() > 1;
        }

        final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode);
        targetNode.remove(index);

        nodesManager.returnMappedNode(mappedNode);

        updatePathToRoot(parentCursor, targetNode);
    }

    /**
     * Inserts a new key/value into the BTree. Grows the tree if needed.
     * If the key already exists, overrides the value for it
     *
     * @param key   the kay to insert/set
     * @param value the value
     */
    public void put(final long key, final long value)
    {
        BTreeNode rootNode = getCurrentRootNode();
        final CursorPosition cursorPosition = traverseDown(rootNode, key);

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

        BTreeNode targetNode = cursorPosition.getNode(mappedNode);
        CursorPosition parentCursor = cursorPosition.parent;

        BTreeNodeHeap currentNode = nodesManager.copyNode(targetNode);
        currentNode.insert(key, value);

        int keyCount = currentNode.getKeyCount();
        while (keyCount > Config.MAX_CHILDREN_PER_NODE)
        {
            this.nodesCount++;
            final int at = keyCount >> 1;
            final long keyAt = currentNode.getKey(at);
            final BTreeNodeHeap split = nodesManager.splitNode(currentNode, at);

            if (parentCursor == null)
            {
                this.nodesCount++;
                final BTreeNodeHeap temp = nodesManager.createEmptyNonLeafNode();

                temp.insertChild(0, keyAt, currentNode);
                temp.setChild(1, split);

                currentNode = temp;

                break;
            }

            final BTreeNodeHeap parentNode = nodesManager.copyNode(parentCursor.getNode(mappedNode));
            parentNode.setChild(parentCursor.index, split);
            parentNode.insertChild(parentCursor.index, keyAt, currentNode);

            parentCursor = parentCursor.parent;
            currentNode = parentNode;
            keyCount = currentNode.getKeyCount();
        }

        nodesManager.returnMappedNode(mappedNode);

        updatePathToRoot(parentCursor, currentNode);
    }

    /**
     * Gets a value for the key at time/instance t.
     *
     * @param key     the key to search for
     * @param version the version that we are interested. Must be >= 0
     */
    public long get(final long key, final int version)
    {
        assert version >= 0;

        //TODO optimize, we don't need the whole path, just the end node.
        RootReference rootReference = getRootReferenceForVersion(version);
        if (rootReference == null)
        {
            return -1;
        }

        final CursorPosition cursorPosition = traverseDown(rootReference.root, key);

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        final long value = cursorPosition.getNode(mappedNode).get(key);
        nodesManager.returnMappedNode(mappedNode);

        return value;
    }

    public long get(final long key)
    {
        //TODO optimize, we don't need the whole path, just the end node.
        final CursorPosition cursorPosition = traverseDown(getCurrentRootNode(), key);
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        final long value = cursorPosition.getNode(mappedNode).get(key);
        nodesManager.returnMappedNode(mappedNode);
        return value;
    }

    /**
     * Calls the consumer for all the key/value pairs in linear scan, from start to end.
     * @param consumer A consumer that will accept the key/value pairs
     */
    public void consumeAll(final BiConsumer<Long, Long> consumer)
    {
        final BTreeNode rootNodeForVersion = getCurrentRootNode();

        if (rootNodeForVersion.isInternal())
        {
            consumeNonLeafNode(consumer, rootNodeForVersion);
        }
        else
        {
            consumeLeafNode(consumer, rootNodeForVersion);
        }
    }

    /**
     * Calls the consumer for all the key/value pairs in linear scan, from start to end.
     * @param version Version that we want to scan for
     * @param consumer A consumer that will accept the key/value pairs
     */
    public void consumeAll(final int version, final BiConsumer<Long, Long> consumer)
    {
        assert version >= 0;

        final BTreeNode rootNodeForVersion = getRootNodeForVersion(version);

        if (rootNodeForVersion.isInternal())
        {
            consumeNonLeafNode(consumer, rootNodeForVersion);
        }
        else
        {
            consumeLeafNode(consumer, rootNodeForVersion);
        }
    }

    public void commit()
    {
        nodesManager.commitDirtyNodes();
        nodesManager.commitLastRootPage(getCurrentRootNode().getPageNumber());
    }

    public void close()
    {
        nodesManager.close();
    }

    public String print()
    {
        return BTreePrinter.print(this, nodesManager);
    }

    private void consumeNonLeafNode(final BiConsumer<Long, Long> consumer, final BTreeNode nonLeaf)
    {
        assert nonLeaf != null;

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

        final int childPageCount = nonLeaf.getChildrenNumber();
        for (int i = 0; i < childPageCount; i++)
        {
            final BTreeNode childPage = nodesManager.loadNode(i, nonLeaf, mappedNode);
            if (childPage.isInternal())
            {
                consumeNonLeafNode(consumer, childPage);
            }
            else
            {
                consumeLeafNode(consumer, childPage);
            }
        }

        nodesManager.returnMappedNode(mappedNode);
    }

    private void consumeLeafNode(BiConsumer<Long, Long> consumer, BTreeNode leaf)
    {
        assert leaf != null;

        final int keyCount = leaf.getKeyCount();
        for (int i = 0; i < keyCount; i++)
        {
            final long key = leaf.getKey(i);
            final long value = leaf.getValue(i);

            consumer.accept(key, value);
        }
    }

    private CursorPosition traverseDown(final BTreeNode root, final long key)
    {
        BTreeNode node = root;
        CursorPosition cursor = null;
        int index;

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        while (node.isInternal())
        {
            assert node.getKeyCount() > 0
                    : String.format("non leaf node should always have at least 1 key. Current node had %d", node.getKeyCount());
            index = node.getKeyIndex(key);
            cursor = createCursorPosition(node, index, cursor);
            node = nodesManager.loadNode(index, node, mappedNode);
        }

        index = node.getKeyIndex(key);
        cursor = createCursorPosition(node, index, cursor);
        nodesManager.returnMappedNode(mappedNode);

        return cursor;
    }

    private CursorPosition createCursorPosition(
            final BTreeNode node,
            final int index,
            final CursorPosition parentCursor)
    {
        if (node instanceof BTreeMappedNode)
        {
            return new CursorPosition(null, node.getPageNumber(), index, parentCursor);
        }
        else
        {
            return new CursorPosition(node, -1, index, parentCursor);
        }
    }

    private void updatePathToRoot(final CursorPosition cursor, final BTreeNodeHeap current)
    {
        BTreeNodeHeap currentNode = current;
        CursorPosition parentCursor = cursor;

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        while (parentCursor != null)
        {
            BTreeNodeHeap c = currentNode;
            currentNode = nodesManager.copyNode(parentCursor.getNode(mappedNode));
            currentNode.setChild(parentCursor.index, c);
            parentCursor = parentCursor.parent;
        }
        nodesManager.returnMappedNode(mappedNode);

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

    private BTreeNode getRootNodeForVersion(int version)
    {
        final RootReference rootReferenceForVersion = getRootReferenceForVersion(version);
        assert rootReferenceForVersion != null;
        assert rootReferenceForVersion.root != null;
        return rootReferenceForVersion.root;
    }

    private RootReference getCurrentRoot()
    {
        return root.get();
    }

    BTreeNode getCurrentRootNode()
    {
        RootReference rootReference = root.get();
        assert rootReference != null;
        return rootReference.root;
    }

    /**
     * Try to set the new root reference from now on.
     *
     * @param oldRoot     previous root reference
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

        if (success)
        {
            nodesManager.addDirtyRoot(newRootPage);
            return updatedRootReference;
        }
        else
        {
            return null;
        }
    }

    public long getNodesCount()
    {
        return this.nodesCount;
    }

    private static final class RootReference
    {
        /**
         * The root page.
         */
        final BTreeNode root;
        /**
         * The version used for writing.
         */
        final long version;
        /**
         * Reference to the previous root in the chain.
         */
        final RootReference previous;

        RootReference(BTreeNode root, long version, RootReference previous)
        {
            this.root = root;
            this.version = version;
            this.previous = previous;
        }
    }
}
