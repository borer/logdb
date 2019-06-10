package org.logdb.bbtree;

import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

public final class RootReference
{
    private long pageNumber;

    /**
     * The root page.
     */
    public final BTreeNodeHeap root;
    /**
     * The version used for writing.
     */
    public final @Milliseconds long timestamp;
    /**
     * The version used for writing.
     */
    public final @Version long version;

    /**
     * Reference to the previous root in the chain.
     */
    public final RootReference previous;

    RootReference(final BTreeNodeHeap root,
                  final @Milliseconds long timestamp,
                  final @Version long version,
                  final RootReference previous)
    {
        this.pageNumber = -1;
        this.root = root;
        this.timestamp = timestamp;
        this.version = version;
        this.previous = previous;
    }

    public void setPageNumber(final long pageNumber)
    {
        this.pageNumber = pageNumber;
    }

    public long getPageNumber()
    {
        return pageNumber;
    }

    RootReference getRootReferenceForVersion(final @Version long version)
    {
        RootReference rootReference = this;
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
}