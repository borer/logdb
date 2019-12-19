package org.logdb.bbtree;

import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

abstract class BTreeAbstract implements BTree
{
    /**
     * Reference to the current uncommitted root page.
     */
    final AtomicReference<RootReference> uncommittedRoot;
    /**
     * Reference to the current committed root page number.
     */
    final AtomicReference<Long> committedRoot;

    protected final NodesManager nodesManager;
    private final TimeSource timeSource;

    long nodesCount;

    @Version long nextWriteVersion;

    BTreeAbstract(
            final NodesManager nodesManager,
            final TimeSource timeSource,
            final @Version long nextWriteVersion,
            final @PageNumber long lastRootPageNumber,
            final RootReference rootReference)
    {
        this.nodesManager = Objects.requireNonNull(nodesManager, "nodesManager must not be null");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource must not be null");
        this.nextWriteVersion = nextWriteVersion;

        this.committedRoot = new AtomicReference<>(lastRootPageNumber);
        this.uncommittedRoot = new AtomicReference<>(rootReference);

        this.nodesCount = 1;
    }

    @Override
    public void commit() throws IOException
    {
        nodesManager.commitDirtyNodes();

        final RootReference uncommittedRootReference = uncommittedRoot.get();
        if (uncommittedRootReference != null)
        {
            final @PageNumber long pageNumber = uncommittedRootReference.getPageNumber();
            final @Version long version = uncommittedRootReference.version;
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
    public @PageNumber long getCommittedRoot()
    {
        final @PageNumber long committedRootPageNumber = StorageUnits.pageNumber(committedRoot.get());
        assert committedRootPageNumber > 0 : "Committed root page number must be positive. Current " + committedRootPageNumber;
        return committedRootPageNumber;
    }

    CursorPosition getLastCursorPosition(final long key)
    {
        CursorPosition cursorPosition;
        final RootReference rootReference = uncommittedRoot.get();
        if (rootReference != null)
        {
            cursorPosition = traverseDown(rootReference.root, key);
        }
        else
        {
            cursorPosition = traverseDown(StorageUnits.pageNumber(committedRoot.get()), key);
        }
        return cursorPosition;
    }

    CursorPosition traverseDown(final BTreeNode root, final long key)
    {
        BTreeNode node = root;
        CursorPosition cursor = null;
        int index;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            while (node.getNodeType() == BtreeNodeType.NonLeaf)
            {
                assert node.getPairCount() > 0
                        : String.format("non leaf node should always have at least 1 key. Current node had %d", node.getPairCount());
                index = node.getKeyIndex(key);
                cursor = createCursorPosition(node, index, cursor);
                node = nodesManager.loadNode(index, node, mappedNode);
            }

            index = node.getKeyIndex(key);
            cursor = createCursorPosition(node, index, cursor);
        }

        return cursor;
    }

    private CursorPosition traverseDown(final @PageNumber long rootPageNumber, final long key)
    {
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            mappedNode.initNode(rootPageNumber);

            BTreeNode node = mappedNode;
            CursorPosition cursor = null;
            int index;

            while (node.getNodeType() == BtreeNodeType.NonLeaf)
            {
                assert node.getPairCount() > 0
                        : String.format("non leaf node should always have at least 1 key. Current node had %d", node.getPairCount());
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
            return new CursorPosition(node, StorageUnits.INVALID_PAGE_NUMBER, index, parentCursor);
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

    BTreeNode getRootNode(final @Version long version, final BTreeMappedNode mappedNode)
    {
        final BTreeNode rootForVersion;
        final RootReference currentRootReference = uncommittedRoot.get();
        if (currentRootReference != null)
        {
            final RootReference rootNodeForVersion = currentRootReference.getRootReferenceForVersion(version);
            if (rootNodeForVersion != null)
            {
                rootForVersion = rootNodeForVersion.root;
            }
            else
            {
                final @PageNumber long committedRootPageNumber = nodesManager.getPageNumberForVersion(version);
                mappedNode.initNode(committedRootPageNumber);
                rootForVersion = mappedNode;
            }
        }
        else
        {
            final @PageNumber long committedRootPageNumber = nodesManager.getPageNumberForVersion(version);
            mappedNode.initNode(committedRootPageNumber);
            rootForVersion = mappedNode;
        }

        return rootForVersion;
    }

    BTreeNode getRootNodeByTimestamp(final @Milliseconds long timestamp, final BTreeMappedNode mappedNode)
    {
        final BTreeNode rootForVersion;
        final RootReference currentRootReference = uncommittedRoot.get();
        if (currentRootReference != null)
        {
            final RootReference rootNodeForVersion = currentRootReference.getRootReferenceForTimestamp(timestamp);
            if (rootNodeForVersion != null)
            {
                rootForVersion = rootNodeForVersion.root;
            }
            else
            {
                final @PageNumber long committedRootPageNumber = nodesManager.getPageNumberForTimestamp(timestamp);
                mappedNode.initNode(committedRootPageNumber);
                rootForVersion = mappedNode;
            }
        }
        else
        {
            final @PageNumber long committedRootPageNumber = nodesManager.getPageNumberForTimestamp(timestamp);
            mappedNode.initNode(committedRootPageNumber);
            rootForVersion = mappedNode;
        }

        return rootForVersion;
    }

    @Override
    public long getNodesCount()
    {
        return this.nodesCount;
    }
}
