package org.logdb.bit;

import java.util.Arrays;

public class UnsafeArrayList<S>
{
    private S[] storageArray;
    private int maxValidIndex;

    public UnsafeArrayList(final S[] storageArray)
    {
        this.storageArray = storageArray;
        this.maxValidIndex = storageArray.length - 1;
    }

    public void add(final S element)
    {
        if (maxValidIndex == storageArray.length - 1)
        {
            final int newLength = Math.max(1, storageArray.length * 2);
            storageArray = Arrays.copyOf(storageArray, newLength);
        }

        storageArray[maxValidIndex + 1] = element;
        ++maxValidIndex;
    }

    public S get(final int index)
    {
        assert index >= 0;
        assert index < storageArray.length;
        assert index <= maxValidIndex;

        return storageArray[index];
    }

    public int size()
    {
        return maxValidIndex + 1;
    }

    public void clean()
    {
        Arrays.fill(storageArray, null);
        maxValidIndex = 0;
    }
}
