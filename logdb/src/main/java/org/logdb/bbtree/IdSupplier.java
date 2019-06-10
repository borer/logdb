package org.logdb.bbtree;

import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;

import java.util.function.LongSupplier;

public class IdSupplier implements LongSupplier
{
    private @PageNumber long id;

    public IdSupplier()
    {
        this.id = StorageUnits.pageNumber(0);
    }

    public IdSupplier(final @PageNumber long id)
    {
        this.id = id;
    }

    @Override
    public @PageNumber long getAsLong()
    {
        final @PageNumber long idToReturn = id;
        id++;
        return idToReturn;
    }
}
