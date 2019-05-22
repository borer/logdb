package org.logdb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class FileUtils
{
    /**
     * Read data from the channel to the given byte buffer until there are no bytes remaining in the buffer or the end
     * of the file has been reached.
     *
     * @param channel File channel containing the data to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; it must be non-negative
     *
     * @throws IllegalArgumentException If position is negative
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)} for details on the
     *                  possible exceptions
     */
    static void readFully(final FileChannel channel,
                          final ByteBuffer destinationBuffer,
                          final long position) throws IOException
    {
        if (position < 0)
        {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        long currentPosition = position;
        int bytesRead;
        do
        {
            bytesRead = channel.read(destinationBuffer, currentPosition);
            currentPosition += bytesRead;
        }
        while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

    static void writeFully(final FileChannel channel, final ByteBuffer sourceBuffer) throws IOException
    {
        sourceBuffer.mark();
        while (sourceBuffer.hasRemaining())
        {
            channel.write(sourceBuffer);
        }
        sourceBuffer.reset();
    }
}
