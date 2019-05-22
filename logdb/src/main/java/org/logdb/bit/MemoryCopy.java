package org.logdb.bit;

import java.nio.ByteBuffer;
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
            final ByteBuffer sourceSupportByteBufferIfAny = sourceMemory.getSupportByteBufferIfAny();
            final byte[] sourceArray =
                    (sourceSupportByteBufferIfAny != null && !sourceSupportByteBufferIfAny.isDirect())
                            ? sourceSupportByteBufferIfAny.array()
                            : null;

            final ByteBuffer destinationSupportByteBufferIfAny = destinationMemory.getSupportByteBufferIfAny();
            final byte[] destinationArray =
                    (destinationSupportByteBufferIfAny != null && !destinationSupportByteBufferIfAny.isDirect())
                            ? destinationSupportByteBufferIfAny.array()
                            : null;

            NativeMemoryAccess.copyBytes(
                    sourceArray,
                    sourceMemory.getBaseAddress() + sourceOffset,
                    destinationArray,
                    destinationMemory.getBaseAddress() + destinationOffset,
                    lengthBytes);
        }
    }
}
