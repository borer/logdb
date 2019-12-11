package org.logdb.async;

final class Command
{
    final CommandType commandType;
    final long key;
    final long value;

    Command(final CommandType commandType, final long key, final long value)
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
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
