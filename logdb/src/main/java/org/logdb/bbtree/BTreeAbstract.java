package org.logdb.bbtree;

import org.logdb.storage.NodesManager;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

abstract class BTreeAbstract implements BTree
{
    /**
     * This designates the "last stored" version for a store which was
     * just open for the first time.
     */
    private static final long INITIAL_VERSION = 0;

    /**
     * Reference to the current uncommitted root page.
     */
    final AtomicReference<RootReference> uncommittedRoot;
    /**
     * Reference to the current committed root page number.
     */
    final AtomicReference<Long> committedRoot;

    protected final NodesManager nodesManager;
    final TimeSource timeSource;

    long nodesCount;

    long writeVersion;

    BTreeAbstract(final NodesManager nodesManager, final TimeSource timeSource)
    {
        this.nodesManager = Objects.requireNonNull(
                nodesManager, "nodesManager must not be null");
        this.timeSource = timeSource;
        final long lastRootPageNumber = nodesManager.loadLastRootPageNumber();
        this.committedRoot = new AtomicReference<>(lastRootPageNumber);

        final boolean isNewBtree = lastRootPageNumber < 0;
        if (isNewBtree)
        {
            final RootReference rootReference = new RootReference(
                    nodesManager.createEmptyLeafNode(),
                    timeSource.getCurrentMillis(),
                    INITIAL_VERSION,
                    null);
            this.uncommittedRoot = new AtomicReference<>(rootReference);
            this.writeVersion = INITIAL_VERSION;

            nodesManager.addDirtyRoot(rootReference);
        }
        else
        {
            this.uncommittedRoot = new AtomicReference<>(null);
            final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
            mappedNode.initNode(lastRootPageNumber);
            this.writeVersion = mappedNode.getVersion();
        }

        this.nodesCount = 1;
    }

    @Override
    public void commit()
    {
        nodesManager.commitDirtyNodes();

        final RootReference uncommittedRootReference = uncommittedRoot.get();
        if (uncommittedRootReference != null)
        {
            final long pageNumber = uncommittedRootReference.getPageNumber();
            final long version = uncommittedRootReference.version;
            nodesManager.commitLastRootPage(pageNumber, version);

            uncommittedRoot.set(null);
            committedRoot.set(pageNumber);
        }
    }

    @Override
    public void close()
    {
        nodesManager.close();
    }

    @Override
    public String print()
    {
        return BTreePrinter.print(this, nodesManager);
    }

    @Override
    public BTreeNode getUncommittedRoot()
    {
        final RootReference rootReference = uncommittedRoot.get();
        return rootReference == null ? null : rootReference.root;
    }

    @Override
    public long getCommittedRoot()
    {
        final long committedRootPageNumber = committedRoot.get();
        assert committedRootPageNumber > 0 : "Committed root page number must be positive. Current " + committedRootPageNumber;
        return committedRootPageNumber;
    }

    protected CursorPosition getLastCursorPosition(long key)
    {
        CursorPosition cursorPosition;
        final RootReference rootReference = uncommittedRoot.get();
        if (rootReference != null)
        {
            cursorPosition = traverseDown(rootReference.root, key);
        }
        else
        {
            cursorPosition = traverseDown(committedRoot.get(), key);
        }
        return cursorPosition;
    }

    protected CursorPosition traverseDown(final BTreeNode root, final long key)
    {
        BTreeNode node = root;
        CursorPosition cursor = null;
        int index;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            while (node.getNodeType() == BtreeNodeType.NonLeaf)
            {
                assert node.getKeyCount() > 0
                        : String.format("non leaf node should always have at least 1 key. Current node had %d", node.getKeyCount());
                index = node.getKeyIndex(key);
                cursor = createCursorPosition(node, index, cursor);
                node = nodesManager.loadNode(index, node, mappedNode);
            }

            index = node.getKeyIndex(key);
            cursor = createCursorPosition(node, index, cursor);
        }

        return cursor;
    }

    protected CursorPosition traverseDown(final long rootPageNumber, final long key)
    {
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            mappedNode.initNode(rootPageNumber);

            BTreeNode node = mappedNode;
            CursorPosition cursor = null;
            int index;

            while (node.getNodeType() == BtreeNodeType.NonLeaf)
            {
                assert node.getKeyCount() > 0
                        : String.format("non leaf node should always have at least 1 key. Current node had %d", node.getKeyCount());
                index = node.getKeyIndex(key);
                cursor = createCursorPosition(node, index, cursor);
                node = nodesManager.loadNode(index, node, mappedNode);
            }

            index = node.getKeyIndex(key);
            cursor = createCursorPosition(node, index, cursor);
            return cursor;
        }
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

    /**
     * Try to set the new uncommittedRoot reference from now on.
     *
     * @param newRootPage the new uncommittedRoot page
     * @return new RootReference or null if update failed
     */
    RootReference setNewRoot(final BTreeNodeHeap newRootPage)
    {
        Objects.requireNonNull(newRootPage, "current uncommittedRoot cannot be null");
        final RootReference currentRoot = uncommittedRoot.get();

        final @Milliseconds long timestamp = timeSource.getCurrentMillis();
        final RootReference updatedRootReference = new RootReference(newRootPage, timestamp, newRootPage.getVersion(), currentRoot);
        boolean success = uncommittedRoot.compareAndSet(currentRoot, updatedRootReference);

        if (success)
        {
            nodesManager.addDirtyRoot(updatedRootReference);
            return updatedRootReference;
        }
        else
        {
            return null;
        }
    }

    @Override
    public long getNodesCount()
    {
        return this.nodesCount;
    }
}
