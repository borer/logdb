package org.logdb.storage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

class FileUtils
{
    /**
     * Read data from the channel to the given byte buffer until there are no bytes remaining in the buffer or the end
     * of the file has been reached.
     *
     * @param channel File channel containing the data to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred
     *
     * @throws IllegalArgumentException If position is negative
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)} for details on the
     *                  possible exceptions
     */
    static void readFully(final ReadableByteChannel channel,
                          final ByteBuffer destinationBuffer) throws IOException
    {
        int bytesRead;
        do
        {
            bytesRead = channel.read(destinationBuffer);
        }
        while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

    static void writeFully(final WritableByteChannel channel, final ByteBuffer sourceBuffer) throws IOException
    {
        sourceBuffer.mark();
        while (sourceBuffer.hasRemaining())
        {
            channel.write(sourceBuffer);
        }
        sourceBuffer.reset();
    }
}
