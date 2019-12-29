package org.logdb.async;

final class Command
{
    final CommandType commandType;
    final byte[] key;
    final byte[] value;

    Command(final CommandType commandType, final byte[] key, final byte[] value)
    {
        this.commandType = commandType;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString()
    {
        return "Command{" +
                "commandType=" + commandType +
                ", key=" + new String(key) +
                ", value=" + new String(value) +
                '}';
    }
}
