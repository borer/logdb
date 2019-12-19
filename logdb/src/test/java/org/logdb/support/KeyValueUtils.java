package org.logdb.support;

import org.logdb.storage.ByteSize;

import java.util.function.BiConsumer;

public class KeyValueUtils
{
    public static void generateKeyValuePairs(final int numberOfPairsToGenerate, BiConsumer<Integer, Pair> consumer)
    {
        generateKeyValuePairs(numberOfPairsToGenerate, 0, consumer);
    }

    public static void generateKeyValuePairs(final int numberOfPairsToGenerate, final int startOffset, BiConsumer<Integer, Pair> consumer)
    {
        for (int i = 0; i < numberOfPairsToGenerate; i++)
        {
            final int pairStep = startOffset + i;
            final byte[] key = ("k" + pairStep).getBytes();
            final byte[] value = ("v" + pairStep).getBytes();

            consumer.accept(i, new Pair(key, value));
        }
    }

    public static Pair generateKeyValuePair(final int index)
    {
        return generateKeyValuePair(index, "", "");
    }

    public static Pair generateKeyValuePair(final int index, final String keyPrefix, final String valuePrefix)
    {
        final byte[] key = (keyPrefix + "k" + index).getBytes();
        final byte[] value = (valuePrefix + "v" + index).getBytes();

        return new Pair(key, value);
    }

    public final static class Pair
    {
        public final byte[] key;
        public final byte[] value;

        public Pair(byte[] key, byte[] value)
        {
            this.key = key;
            this.value = value;
        }

        public @ByteSize int getTotalLength()
        {
            return key.length + value.length;
        }
    }
}
