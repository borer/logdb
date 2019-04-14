package org.borer.logdb.bbtree;

public final class RootReference
{
    private long pageNumber;

    /**
     * The root page.
     */
    public final BTreeNode root;
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

    RootReference(final BTreeNode root,
                  final long timestamp,
                  final long version,
                  final RootReference previous)
    {
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
}