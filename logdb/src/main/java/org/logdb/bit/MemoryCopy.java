package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;

import java.nio.ByteBuffer;
import java.util.Objects;

import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class MemoryCopy
{
    public static void copy(final Memory sourceMemory, final Memory destinationMemory)
    {
        copy(sourceMemory, destinationMemory, sourceMemory.getCapacity());
    }

    public static void copy(final Memory sourceMemory, final Memory destinationMemory, final @ByteSize long lengthBytes)
    {
        copy(sourceMemory, ZERO_OFFSET, destinationMemory, ZERO_OFFSET, lengthBytes);
    }

    public static void copy(final Memory sourceMemory,
                            final @ByteOffset long sourceOffset,
                            final Memory destinationMemory,
                            final @ByteOffset long destinationOffset,
                            final @ByteSize long lengthBytes)
    {
        Objects.requireNonNull(sourceMemory, "source memory cannot be null");
        Objects.requireNonNull(destinationMemory, "destination memory cannot be null");

        sourceMemory.assertBounds(sourceOffset, lengthBytes);
        destinationMemory.assertBounds(destinationOffset, lengthBytes);

        if ((sourceMemory instanceof MemoryDirectNonNativeImpl && destinationMemory instanceof MemoryDirectImpl) ||
                (sourceMemory instanceof MemoryDirectImpl && destinationMemory instanceof MemoryDirectNonNativeImpl))
        {
            throw new RuntimeException("Copying between non-native and native memory order is not allowed.");
        }
        else
        {
            final byte[] sourceArray = getBackingArray(sourceMemory);
            final byte[] destinationArray = getBackingArray(destinationMemory);

            NativeMemoryAccess.copyBytes(
                    sourceArray,
                    sourceMemory.getBaseAddress() + sourceOffset,
                    destinationArray,
                    destinationMemory.getBaseAddress() + destinationOffset,
                    lengthBytes);
        }
    }

    private static byte[] getBackingArray(final Memory memory)
    {
        final ByteBuffer supportingByteBuffer = memory instanceof HeapMemory
                ? ((HeapMemory) memory).getSupportByteBuffer()
                : null;

        return supportingByteBuffer != null && !supportingByteBuffer.isDirect()
                ? supportingByteBuffer.array()
                : null;
    }
}
