package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;

public class InvalidBTreeValues
{
    public static byte[] KEY_NOT_FOUND_VALUE = BinaryHelper.longToBytes(-99L); //TODO: convert to NULL
    public static long KEY_NOT_FOUND = -99L;
}
