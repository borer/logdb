package org.logdb.logfile;

public enum LogRecordType
{
    UPDATE('U'),
    DELETE('D');

    private final char c;

    LogRecordType(final char c)
    {
        this.c = c;
    }

    public char getChar()
    {
        return c;
    }

    public static LogRecordType fromChar(final char c)
    {
        switch (c)
        {
            case 'U':
                return UPDATE;
            case 'D':
                return DELETE;
            default:
                throw new IllegalArgumentException("Unable to parse log record type " + c);
        }
    }
}
