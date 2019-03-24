package org.borer.logdb.bbtree;

public enum BtreeNodeType
{
    Leaf((byte)0),
    NonLeaf((byte)1);

    private final byte type;

    BtreeNodeType(byte type)
    {
        this.type = type;
    }

    public byte getType()
    {
        return type;
    }

    public BtreeNodeType fromByte(final byte type)
    {
        if (type == 0)
        {
            return Leaf;
        }
        else
        {
            return NonLeaf;
        }
    }
}
