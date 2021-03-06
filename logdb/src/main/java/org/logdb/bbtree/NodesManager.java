package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.bit.HeapMemory;
import org.logdb.root.index.RootIndex;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NodesManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodesManager.class);

    private final Storage storage;
    private final IdSupplier idSupplier;
    private final RootIndex rootIndex;
    private final boolean shouldSyncWrite;
    private final @ByteSize int maxLogSize;

    private final List<RootReference> dirtyRootNodes;
    private final Queue<BTreeNodeNonLeaf> nonLeafNodesCache;
    private final Queue<BTreeNodeLeaf> leafNodesCache;
    private final Queue<BTreeMappedNode> mappedNodes;

    private @PageNumber long lastPersistedPageNumber;

    public NodesManager(
            final Storage storage,
            final RootIndex rootIndex,
            final boolean shouldSyncWrite,
            final @ByteSize int maxLogSize)
    {
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.rootIndex = Objects.requireNonNull(rootIndex, "rootIndex cannot be null");
        this.shouldSyncWrite = shouldSyncWrite;
        this.maxLogSize = maxLogSize;

        this.idSupplier = new IdSupplier();
        this.dirtyRootNodes = new ArrayList<>();
        this.nonLeafNodesCache = new ConcurrentLinkedQueue<>();
        this.leafNodesCache = new ConcurrentLinkedQueue<>();
        this.mappedNodes = new ConcurrentLinkedQueue<>();
        this.lastPersistedPageNumber = StorageUnits.INVALID_PAGE_NUMBER;
    }

    public BTreeNodeLeaf createEmptyLeafNode()
    {
        return getOrCreateLeafNode();
    }

    BTreeNodeNonLeaf createEmptyNonLeafNode()
    {
        return getOrCreateNonLeafNode();
    }

    BTreeMappedNode getOrCreateMappedNode()
    {
        BTreeMappedNode mappedNode = mappedNodes.poll();

        if (mappedNode == null)
        {
            mappedNode = new BTreeMappedNode(
                    this::returnMappedNode,
                    storage,
                    storage.getUninitiatedDirectMemoryPage(),
                    StorageUnits.pageNumber(0),
                    maxLogSize);
        }

        return mappedNode;
    }

    private void returnMappedNode(final BTreeMappedNode mappedNode)
    {
        mappedNodes.add(mappedNode);
    }

    BTreeNodeHeap splitNode(
            final BTreeNode originalNode,
            final int at,
            final @Version long newVersion)
    {
        final BTreeNodeHeap splitNode = createSameNodeType(originalNode);
        originalNode.split(at, splitNode);
        splitNode.setVersion(newVersion);
        originalNode.setVersion(newVersion);
        return splitNode;
    }

    BTreeNodeHeap copyNode(final BTreeNode originalNode, final @Version long newVersion)
    {
        final BTreeNodeHeap copyNode = createSameNodeType(originalNode);
        originalNode.copy(copyNode);
        copyNode.setVersion(newVersion);
        return copyNode;
    }

    private BTreeNodeHeap createSameNodeType(final BTreeNode originalNode)
    {
        if (originalNode.getNodeType() == BtreeNodeType.NonLeaf)
        {
            return getOrCreateNonLeafNode();
        }
        else
        {
            return getOrCreateLeafNode();
        }
    }

    private BTreeNodeNonLeaf getOrCreateNonLeafNode()
    {
        BTreeNodeNonLeaf nonLeaf = nonLeafNodesCache.poll();
        if (nonLeaf == null)
        {
            nonLeaf = new BTreeNodeNonLeaf(
                    idSupplier.getAsLong(),
                    storage.allocateHeapPage(),
                    maxLogSize,
                    1,
                    new BTreeNodeHeap[1]);
        }
        else
        {
            nonLeaf.reset();
        }

        return nonLeaf;
    }

    private BTreeNodeLeaf getOrCreateLeafNode()
    {
        BTreeNodeLeaf leaf = leafNodesCache.poll();
        if (leaf == null)
        {
            leaf = new BTreeNodeLeaf(idSupplier.getAsLong(), storage.allocateHeapPage(), 0);
        }

        else
        {
            leaf.reset();
        }

        return leaf;
    }

    void addDirtyRoot(final RootReference rootNode)
    {
        dirtyRootNodes.add(rootNode);
    }

    /**
     * Stores into persistent storage all the dirty roots and their paths.
     */
    void commitDirtyNodes() throws IOException
    {
        if (dirtyRootNodes.isEmpty())
        {
            return;
        }

        final @PageNumber long lastRootPageNumber = loadLastRootPageNumber();

        //Note: dirty nodes are sorted by version (previous root is always committed before current).
        // That is because of the way they are inserted and because the array list they are stored in, preserves the insertion order.
        for (int i = 0; i < dirtyRootNodes.size(); ++i)
        {
            final RootReference dirtyRootNode = dirtyRootNodes.get(i);

            final @PageNumber long previousRootPageNumber = dirtyRootNode.previous == null
                    ? lastRootPageNumber
                    : dirtyRootNode.previous.getPageNumber();
            final @PageNumber long pageNumber = dirtyRootNode.root.commit(
                    this,
                    true,
                    previousRootPageNumber,
                    dirtyRootNode.timestamp,
                    dirtyRootNode.version);

            dirtyRootNode.setPageNumber(pageNumber);

            lastPersistedPageNumber = pageNumber;

            rootIndex.append(
                    dirtyRootNode.version,
                    dirtyRootNode.timestamp,
                    storage.getOffset(pageNumber));
        }

        if (shouldSyncWrite)
        {
            rootIndex.flush(false);
            storage.flush(false);
        }

        dirtyRootNodes.clear();
    }

    /**
     * After this method, the node should not be used anymore as it's put backed into the pool.
     * @param node the node to commit
     * @return the page number where this node is stored
     */
    @PageNumber long commitNode(final BTreeNodeNonLeaf node) throws IOException
    {
        final @PageNumber long pageNumber = commitNodeToStorage(node);
        node.reset();
        nonLeafNodesCache.add(node);
        return pageNumber;
    }

    /**
     * After this method, the node should not be used anymore as it's put backed into the pool.
     * @param node the node to commit
     * @return the page number where this node is stored
     */
    @PageNumber long commitNode(final BTreeNodeLeaf node) throws IOException
    {
        final @PageNumber long pageNumber = commitNodeToStorage(node);
        node.reset();
        leafNodesCache.add(node);
        return pageNumber;
    }

    private @PageNumber long commitNodeToStorage(final BTreeNodeHeap node) throws IOException
    {
        final HeapMemory buffer = node.getBuffer();
        return storage.appendPageAligned(buffer.getSupportByteBuffer());
    }

    BTreeNode loadNode(final int index, final BTreeNode parentNode, final BTreeMappedNode mappedNode)
    {
        assert parentNode.getNodeType() == BtreeNodeType.NonLeaf : "node must be non leaf";

        final @PageNumber byte[] childPageNumberBytes = StorageUnits.pageNumber(parentNode.getValue(index));
        if (Arrays.equals(childPageNumberBytes, BTreeNodeNonLeaf.NON_COMMITTED_CHILD))
        {
            return parentNode.getChildAt(index);
        }
        else
        {
            final @PageNumber long pageNumber = StorageUnits.pageNumber(BinaryHelper.bytesToLong(childPageNumberBytes));
            mappedNode.initNode(pageNumber);
            return mappedNode;
        }
    }

    void commitLastRootPage(final @PageNumber long pageNumber, final @Version long version)
    {
        final @ByteOffset long offset = storage.getOffset(pageNumber);
        storage.commitMetadata(offset, version);
    }

    public @PageNumber long loadLastRootPageNumber()
    {
        if (lastPersistedPageNumber == StorageUnits.INVALID_PAGE_NUMBER)
        {
            lastPersistedPageNumber = storage.getLastPersistedPageNumber();
        }

        return lastPersistedPageNumber;
    }

    @PageNumber long getPageNumberForVersion(final @Version long version)
    {
        final @ByteOffset long versionOffset = rootIndex.getVersionOffset(version);
        return storage.getPageNumber(versionOffset);
    }

    @PageNumber long getPageNumberForTimestamp(final @Milliseconds long timestamp)
    {
        final @ByteOffset long timestampOffsetOffset = rootIndex.getTimestampOffset(timestamp);
        return storage.getPageNumber(timestampOffsetOffset);

    }

    public void close()
    {
        try
        {
            leafNodesCache.clear();
            nonLeafNodesCache.clear();
            mappedNodes.clear();
            storage.close();
        }
        catch (final Exception e)
        {
            LOGGER.error("Unable to close storage", e);
        }
    }
}
