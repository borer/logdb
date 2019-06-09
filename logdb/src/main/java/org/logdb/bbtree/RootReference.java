package org.logdb.bbtree;

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
    public final long timestamp;
    /**
     * The version used for writing.
     */
    public final long version;

    /**
     * Reference to the previous root in the chain.
     */
    public final RootReference previous;

    RootReference(final BTreeNodeHeap root,
                  final long timestamp,
                  final long version,
                  final RootReference previous)
    {
        this.pageNumber = -1;
        this.root = root;
        this.timestamp = timestamp;
        this.version = version;
        this.previous = previous;
    }

    public RootReference(final long pageNumber, final long timestamp, final long version)
    {
        this.pageNumber = pageNumber;
        this.root = null;
        this.timestamp = timestamp;
        this.version = version;
        this.previous = null;
    }

    public void setPageNumber(final long pageNumber)
    {
        this.pageNumber = pageNumber;
    }

    public long getPageNumber()
    {
        return pageNumber;
    }

    RootReference getRootReferenceForVersion(final long version)
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