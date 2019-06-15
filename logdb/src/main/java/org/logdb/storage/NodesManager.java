package org.logdb.storage;

import org.logdb.bbtree.BTreeMappedNode;
import org.logdb.bbtree.BTreeNode;
import org.logdb.bbtree.BTreeNodeHeap;
import org.logdb.bbtree.BTreeNodeLeaf;
import org.logdb.bbtree.BTreeNodeNonLeaf;
import org.logdb.bbtree.BtreeNodeType;
import org.logdb.bbtree.IdSupplier;
import org.logdb.bbtree.RootReference;
import org.logdb.bit.ReadMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

public class NodesManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodesManager.class);

    private final Storage storage;
    private final IdSupplier idSupplier;

    private final List<RootReference> dirtyRootNodes;
    private final Queue<BTreeNodeNonLeaf> nonLeafNodesCache;
    private final Queue<BTreeNodeLeaf> leafNodesCache;
    private final Queue<BTreeMappedNode> mappedNodes;

    public NodesManager(final Storage storage)
    {
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.idSupplier = new IdSupplier();
        this.dirtyRootNodes = new ArrayList<>();
        this.nonLeafNodesCache = new ArrayDeque<>();
        this.leafNodesCache = new ArrayDeque<>();
        this.mappedNodes = new ArrayDeque<>();
    }

    public BTreeNodeLeaf createEmptyLeafNode()
    {
        return getOrCreateLeafNode();
    }

    public BTreeNodeNonLeaf createEmptyNonLeafNode()
    {
        return getOrCreateNonLeafNode();
    }

    public BTreeMappedNode getOrCreateMappedNode()
    {
        if (!mappedNodes.isEmpty())
        {
            return mappedNodes.poll();
        }
        else
        {
            return new BTreeMappedNode(
                    this,
                    storage,
                    storage.getUninitiatedDirectMemoryPage(),
                    storage.getPageSize(),
                    StorageUnits.pageNumber(0));
        }
    }

    public void returnMappedNode(final BTreeMappedNode mappedNode)
    {
        mappedNodes.add(mappedNode);
    }

    public BTreeNodeHeap splitNode(
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

    public BTreeNodeHeap copyNode(final BTreeNode originalNode, final @Version long newVersion)
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
            nonLeaf = new BTreeNodeNonLeaf(idSupplier.getAsLong(), storage.allocateHeapPage(), 0, 0, 1, new BTreeNodeHeap[1]);
        }

        return nonLeaf;
    }

    private BTreeNodeLeaf getOrCreateLeafNode()
    {
        BTreeNodeLeaf leaf = leafNodesCache.poll();
        if (leaf == null)
        {
            leaf = new BTreeNodeLeaf(idSupplier.getAsLong(), storage.allocateHeapPage(), 0, 0, 0);
        }

        return leaf;
    }

    public void addDirtyRoot(final RootReference rootNode)
    {
        dirtyRootNodes.add(rootNode);
    }

    /**
     * Stores into persistent storage all the dirty roots and their paths.
     */
    public void commitDirtyNodes()
    {
        if (dirtyRootNodes.isEmpty())
        {
            return;
        }

        final @PageNumber long lastRootPageNumber = storage.getPageNumber(storage.getLastPersistedOffset());

        //TODO: make explicit that dirtyNodes are sorted by version (previous root is always committed before current)

        for (final RootReference dirtyRootNode : dirtyRootNodes)
        {
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
        }

        storage.flush();
        dirtyRootNodes.clear();
    }

    /**
     * After this method, the node should not be used anymore as it's put backed into the pool.
     * @param node the node to commit
     * @return the page number where this node is stored
     */
    public @PageNumber long commitNode(final BTreeNodeNonLeaf node)
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
    public @PageNumber long commitNode(final BTreeNodeLeaf node)
    {
        final @PageNumber long pageNumber = commitNodeToStorage(node);
        node.reset();
        leafNodesCache.add(node);
        return pageNumber;
    }

    private @PageNumber long commitNodeToStorage(final BTreeNodeHeap node)
    {
        final ReadMemory buffer = node.getBuffer();
        return storage.writePageAligned(buffer.getSupportByteBufferIfAny());
    }

    public BTreeNode loadNode(final int index, final BTreeNode parentNode, final BTreeMappedNode mappedNode)
    {
        assert parentNode.getNodeType() == BtreeNodeType.NonLeaf : "node must be non leaf";

        final @PageNumber long childPageNumber = StorageUnits.pageNumber(parentNode.getValue(index));
        if (childPageNumber != BTreeNodeNonLeaf.NON_COMMITTED_CHILD)
        {
            mappedNode.initNode(childPageNumber);
            return mappedNode;
        }
        else
        {
            return parentNode.getChildAt(index);
        }
    }

    public void commitLastRootPage(final @PageNumber long pageNumber, final @Version long version)
    {
        final @ByteOffset long offset = storage.getOffset(pageNumber);
        storage.commitMetadata(offset, version);
    }

    public @PageNumber long loadLastRootPageNumber()
    {
        return storage.getPageNumber(storage.getLastPersistedOffset());
    }

    public void close()
    {
        try
        {
            mappedNodes.clear();
            storage.close();
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to close storage", e);
        }
    }
}
