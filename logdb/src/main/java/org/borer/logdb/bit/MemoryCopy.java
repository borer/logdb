package org.borer.logdb.bit;

public class MemoryCopy
{
    public static void copy(final Memory sourceMemory, final Memory destinationMemory)
    {
        final long lengthBytes = sourceMemory.getCapacity();

        destinationMemory.assertBounds(0, lengthBytes);

        if ((sourceMemory instanceof MemoryDirectNonNativeImpl && !(destinationMemory instanceof MemoryDirectNonNativeImpl)) ||
                (!(sourceMemory instanceof MemoryDirectNonNativeImpl) && destinationMemory instanceof MemoryDirectNonNativeImpl))
        {
            throw new RuntimeException("Copying between non-native and native memory order is not allowed.");
        }
        else
        {
            NativeMemoryAccess.copyBytes(
                    sourceMemory.getSupportByteArrayIfAny(),
                    sourceMemory.getBaseAddress(),
                    destinationMemory.getSupportByteArrayIfAny(),
                    destinationMemory.getBaseAddress(),
                    lengthBytes);
        }
    }
}
