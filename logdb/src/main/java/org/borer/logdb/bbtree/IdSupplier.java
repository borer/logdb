package org.borer.logdb.bbtree;

import java.util.function.LongSupplier;

public class IdSupplier implements LongSupplier
{
    private long id;

    public IdSupplier()
    {
        this.id = 0;
    }

    public IdSupplier(long id)
    {
        this.id = id;
    }

    @Override
    public long getAsLong()
    {
        final long idToReturn = id;
        id++;
        return idToReturn;
    }
}
