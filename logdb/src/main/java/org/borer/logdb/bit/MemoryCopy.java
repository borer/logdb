package org.borer.logdb.bit;

public class MemoryCopy
{
    public static void copy(final Memory sourceMemory, final Memory destinationMemory)
    {
        final long lengthBytes = sourceMemory.getCapacity();

        destinationMemory.assertBounds(0, lengthBytes);

        NativeMemoryAccess.copyBytes(
                sourceMemory.unsafeObject(),
                sourceMemory.getBaseAddress(),
                destinationMemory.unsafeObject(),
                destinationMemory.getBaseAddress(),
                lengthBytes);
    }
}
