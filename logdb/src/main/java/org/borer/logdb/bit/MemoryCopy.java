package org.borer.logdb.bit;

import java.util.Objects;

public class MemoryCopy
{
    public static void copy(final Memory sourceMemory, final Memory destinationMemory)
    {
        copy(sourceMemory, 0, destinationMemory, 0, sourceMemory.getCapacity());
    }

    public static void copy(final Memory sourceMemory,
                            final long sourceOffset,
                            final Memory destinationMemory,
                            final long destinationOffset,
                            final long lengthBytes)
    {
        Objects.requireNonNull(sourceMemory, "source memory cannot be null");
        Objects.requireNonNull(destinationMemory, "destination memory cannot be null");

        sourceMemory.assertBounds(sourceOffset, lengthBytes);
        destinationMemory.assertBounds(destinationOffset, lengthBytes);

        if ((sourceMemory instanceof MemoryDirectNonNativeImpl && !(destinationMemory instanceof MemoryDirectNonNativeImpl)) ||
                (!(sourceMemory instanceof MemoryDirectNonNativeImpl) && destinationMemory instanceof MemoryDirectNonNativeImpl))
        {
            throw new RuntimeException("Copying between non-native and native memory order is not allowed.");
        }
        else
        {
            NativeMemoryAccess.copyBytes(
                    sourceMemory.getSupportByteArrayIfAny(),
                    sourceMemory.getBaseAddress() + sourceOffset,
                    destinationMemory.getSupportByteArrayIfAny(),
                    destinationMemory.getBaseAddress() + destinationOffset,
                    lengthBytes);
        }
    }
}
