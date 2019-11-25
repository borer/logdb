package org.logdb.checksum;

public enum ChecksumType
{
    CRC32(1),
    FLETCHER4(2),
    SHA256(3);

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
