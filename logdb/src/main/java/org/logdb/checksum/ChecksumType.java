package org.logdb.checksum;

public enum ChecksumType
{
    CRC32(1),
    FLETCHER8(2),
    FLETCHER32(3),
    SHA256(4);

    private final int type;

    ChecksumType(final int type)
    {

        this.type = type;
    }

    public int getTypeValue()
    {
        return type;
    }

    public static ChecksumType fromValue(final int value)
    {
        for (ChecksumType checksumType : values())
        {
            if (checksumType.type == value)
            {
                return checksumType;
            }
        }

        throw new IllegalArgumentException("Value provided cannot match any consumer type. Provided " + value);
    }
}
