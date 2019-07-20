package org.logdb.storage.file;

import org.junit.jupiter.api.Test;
import org.logdb.bit.UnsafeArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UnsafeArrayListTest
{
    @Test
    void shouldInsertAndRetrieveElements()
    {
        final UnsafeArrayList<Integer> arrayList = new UnsafeArrayList<>(new Integer[0]);

        final int maxElementsToAdd = 19;
        for (int i = 0; i < maxElementsToAdd; i++)
        {
            arrayList.add(i);
        }

        for (int i = 0; i < maxElementsToAdd; i++)
        {
            assertEquals(i, arrayList.get(i));
        }
    }

    @Test
    void shouldHaveCorrectSize()
    {
        final UnsafeArrayList<Integer> arrayList = new UnsafeArrayList<>(new Integer[0]);

        final int maxElementsToAdd = 19;
        for (int i = 0; i < maxElementsToAdd; i++)
        {
            arrayList.add(i);
        }

        assertEquals(maxElementsToAdd, arrayList.size());
    }

    @Test
    void shouldInitializeSizeCorrectly()
    {
        final int maxElementsToAdd = 19;
        final Integer[] storageArray = new Integer[maxElementsToAdd];
        for (int i = 0; i < maxElementsToAdd; i++)
        {
            storageArray[i] = i;
        }

        final UnsafeArrayList<Integer> arrayList = new UnsafeArrayList<>(storageArray);
        assertEquals(maxElementsToAdd, arrayList.size());

        for (int i = 0; i < maxElementsToAdd; i++)
        {
            assertEquals(i, arrayList.get(i));
        }
    }
}