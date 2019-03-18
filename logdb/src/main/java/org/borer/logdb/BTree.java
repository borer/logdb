package org.borer.logdb;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryFactory;
import org.borer.logdb.storage.FileStorage;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.borer.logdb.Config.BYTE_ORDER;
import static org.borer.logdb.Config.PAGE_SIZE_BYTES;

public class BTree
{
    private static final int MAX_CHILDREN_PER_NODE = 10;
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
    private final IdSupplier idSupplier;
    private final FileStorage fileStorage;

    private long nodesCount;

    public BTree(final FileStorage fileStorage)
    {
        this.fileStorage = fileStorage;
        this.idSupplier = new IdSupplier();
        this.root = new AtomicReference<>(null);
        this.nodesCount = 1;

        final Memory memory = MemoryFactory.allocateDirect(PAGE_SIZE_BYTES, BYTE_ORDER);
        final BTreeNodeLeaf emptyLeaf = new BTreeNodeLeaf(
                memory,
                0,
                0,
                idSupplier);
        setNewRoot(null, emptyLeaf);
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

        int index = cursorPosition.index;
        BTreeNode currentNode = cursorPosition.node;
        CursorPosition parentCursor = cursorPosition.parent;

        if (currentNode.getKeyCount() == 1 && parentCursor != null)
        {
            this.nodesCount--;
            index = parentCursor.index;
            currentNode = parentCursor.node;
            parentCursor = parentCursor.parent;

            if (currentNode.getKeyCount() == 1)
            {
                this.nodesCount--;
                assert index <= 1;
                final BTreeNodeNonLeaf currentNonLeafNode = (BTreeNodeNonLeaf) currentNode;
                currentNode = currentNonLeafNode.getChildAtIndex(1 - index);

                updatePathToRoot(parentCursor, currentNode);
                return;
            }
            assert currentNode.getKeyCount() > 1;
        }

        currentNode = currentNode.copy(fileStorage.getWritableMemory());
        currentNode.remove(index);

        updatePathToRoot(parentCursor, currentNode);
    }

    /**
     * Remove a key and the associated value, if the key exists. Does rebalancing of the btree
     *
     * @param key the key (may not be null)
     */
    public void removeWithRebalancing(final long key)
    {
        BTreeNode rootNode = getCurrentRootNode();
        final CursorPosition cursorPosition = traverseDown(rootNode, key);

        BTreeNode currentNode = cursorPosition.node;
        int index = cursorPosition.index;
        CursorPosition parentCursor = cursorPosition.parent;

        currentNode = currentNode.copy(fileStorage.getWritableMemory());
        currentNode.remove(index);

        //rebalance
        if (currentNode.needRebalancing(THRESHOLD_CHILDREN_PER_NODE))
        {
            final CursorPosition rightSiblingCursor = parentCursor.getRightSibling();
            if (rightSiblingCursor != null)
            {
                final BTreeNodeLeaf rightSibling = (BTreeNodeLeaf) rightSiblingCursor.node;
                if (rightSibling.getKeyCount() >= THRESHOLD_CHILDREN_PER_NODE + 2)
                {
                    // we can move one from there to here
                }
                else
                {
                    // we merge the two nodes together
                }

                //Complicated to update both in the same path
                //updatePathToRoot(parentCursor, currentNode, rightSiblingCursor, rightSibling);

                return;
            }

            final CursorPosition leftSiblingCursor = parentCursor.getLeftSibling();
            if (leftSiblingCursor != null)
            {
                final BTreeNodeLeaf leftSibling = (BTreeNodeLeaf) leftSiblingCursor.node;
                if (leftSibling.getKeyCount() >= THRESHOLD_CHILDREN_PER_NODE + 2)
                {
                    // we can move one from there to here
                }
                else
                {
                    // we merge the two nodes together
                }

                //Complicated to update both in the same path
                //updatePathToRoot(parentCursor, currentNode, leftSiblingCursor, leftSibling);

                return;
            }
        }

        updatePathToRoot(parentCursor, currentNode);
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

        BTreeNode currentNode = cursorPosition.node;
        CursorPosition parentCursor = cursorPosition.parent;

        final Memory writableMemory = fileStorage.getWritableMemory();
        currentNode = currentNode.copy(writableMemory);
        currentNode.insert(key, value);

        int keyCount = currentNode.getKeyCount();
        while (keyCount > MAX_CHILDREN_PER_NODE)
        {
            this.nodesCount++;
            final int at = keyCount >> 1;
            final long keyAt = currentNode.getKey(at);
            final BTreeNode split = currentNode.split(at, fileStorage.getWritableMemory());

            if (parentCursor == null)
            {
                this.nodesCount++;

                //TODO: extract memory allocation outside of here
                BTreeNodeNonLeaf temp = new BTreeNodeNonLeaf(
                        fileStorage.getWritableMemory(),
                        0,
                        1,
                        new BTreeNode[1],
                        idSupplier);

                temp.insertChild(0, keyAt, currentNode);
                temp.setChild(1, split);

                currentNode = temp;

                break;
            }

            final BTreeNodeNonLeaf parentNode =
                    (BTreeNodeNonLeaf) parentCursor.node.copy(fileStorage.getWritableMemory());
            parentNode.setChild(parentCursor.index, split);
            parentNode.insertChild(parentCursor.index, keyAt, currentNode);

            parentCursor = parentCursor.parent;
            currentNode = parentNode;
            keyCount = currentNode.getKeyCount();
        }

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

        RootReference rootReference = getRootReferenceForVersion(version);
        if (rootReference == null)
        {
            return -1;
        }

        final BTreeNode rootNode = rootReference.root;
        return rootNode.get(key);
    }

    public long get(final long key)
    {
        final BTreeNode rootNode = getCurrentRootNode();
        return rootNode.get(key);
    }

    /**
     * Calls the consumer for all the key/value pairs in linear scan, from start to end.
     * @param consumer A consumer that will accept the key/value pairs
     */
    public void consumeAll(BiConsumer<Long, Long> consumer)
    {
        final BTreeNode rootNodeForVersion = getCurrentRootNode();

        if (rootNodeForVersion instanceof BTreeNodeNonLeaf)
        {
            consumeNonLeafNode(consumer, (BTreeNodeNonLeaf) rootNodeForVersion);
        }
        else
        {
            consumeLeafNode(consumer, (BTreeNodeLeaf) rootNodeForVersion);
        }
    }

    /**
     * Calls the consumer for all the key/value pairs in linear scan, from start to end.
     * @param version Version that we want to scan for
     * @param consumer A consumer that will accept the key/value pairs
     */
    public void consumeAll(final int version, BiConsumer<Long, Long> consumer)
    {
        assert version >= 0;

        final BTreeNode rootNodeForVersion = getRootNodeForVersion(version);

        if (rootNodeForVersion instanceof BTreeNodeNonLeaf)
        {
            consumeNonLeafNode(consumer, (BTreeNodeNonLeaf) rootNodeForVersion);
        }
        else
        {
            consumeLeafNode(consumer, (BTreeNodeLeaf) rootNodeForVersion);
        }
    }

    private void consumeNonLeafNode(BiConsumer<Long, Long> consumer, BTreeNodeNonLeaf nonLeaf)
    {
        assert nonLeaf != null;

        final int childPageCount = nonLeaf.getRawChildPageCount();
        for (int i = 0; i < childPageCount; i++)
        {
            final BTreeNode childPage = nonLeaf.getChildAtIndex(i);
            if (childPage instanceof BTreeNodeNonLeaf)
            {
                consumeNonLeafNode(consumer, (BTreeNodeNonLeaf) childPage);
            }
            else
            {
                consumeLeafNode(consumer, (BTreeNodeLeaf)childPage);
            }
        }
    }

    private void consumeLeafNode(BiConsumer<Long, Long> consumer, BTreeNodeLeaf leaf)
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

    private static CursorPosition traverseDown(final BTreeNode root, final long key)
    {
        BTreeNode node = root;
        CursorPosition cursor = null;
        int index;
        while (node instanceof BTreeNodeNonLeaf)
        {
            final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;
            assert nonLeaf.getKeyCount() > 0
                    : String.format("non leaf node should always have at least 1 key. Current node had %d",
                                    nonLeaf.getKeyCount());
            index = nonLeaf.binarySearch(key) + 1;
            if (index < 0)
            {
                index = -index;
            }
            cursor = new CursorPosition(node, index, cursor);
            node = nonLeaf.getChildAtIndex(index);
        }

        index = ((BTreeNodeLeaf)node).binarySearch(key);
        if (index < 0)
        {
            index = -index;
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
            currentNode = parentCursor.node.copy(fileStorage.getWritableMemory());
            ((BTreeNodeNonLeaf) currentNode).setChild(parentCursor.index, c);
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
        return success ? updatedRootReference : null;
    }

    public long getNodesCount()
    {
        return this.nodesCount;
    }

    public static final class RootReference
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
