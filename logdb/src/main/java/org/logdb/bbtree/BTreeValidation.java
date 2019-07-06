package org.logdb.bbtree;

import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;

public class BTreeValidation
{
    public static boolean isNewTree(final @PageNumber long lastRootPageNumber)
    {
        return lastRootPageNumber == StorageUnits.INVALID_PAGE_NUMBER;
    }
}
