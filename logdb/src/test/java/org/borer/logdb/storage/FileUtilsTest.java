package org.borer.logdb.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileUtilsTest
{
    private static final String PATHNAME = "io_test";
    private static final String TEST_CONTENT = "this is a test";

    @Test
    void shouldReadFully() throws IOException
    {
        final RandomAccessFile file = new RandomAccessFile(PATHNAME, "rw");
        final FileChannel channel = file.getChannel();

        final ByteBuffer buffer = ByteBuffer.wrap(TEST_CONTENT.getBytes());
        final ByteBuffer readBuffer = ByteBuffer.allocate(buffer.capacity());

        FileUtils.writeFully(channel, buffer);
        FileUtils.readFully(channel, readBuffer, 0);

        assertEquals(TEST_CONTENT, new String(readBuffer.array()));
    }

    @AfterEach
    void tearDown()
    {
        final File file = new File(PATHNAME);
        file.deleteOnExit();
    }
}