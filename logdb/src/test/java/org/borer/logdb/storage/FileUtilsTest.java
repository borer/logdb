package org.borer.logdb.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileUtilsTest
{
    private static final String PATHNAME = "io_test";
    private static final String TEST_CONTENT = "this is a test";

    @TempDir
    Path tempDirectory;

    private FileChannel channel;
    private RandomAccessFile file;

    @BeforeEach
    void setUp() throws FileNotFoundException
    {
        final File tmpFile = tempDirectory.resolve(PATHNAME).toFile();
        file = new RandomAccessFile(tmpFile, "rw");
        channel = file.getChannel();
    }

    @AfterEach
    void tearDown() throws IOException
    {
        channel.close();
        file.close();
    }

    @Test
    void shouldReadFully() throws IOException
    {
        final ByteBuffer buffer = ByteBuffer.wrap(TEST_CONTENT.getBytes());
        final ByteBuffer readBuffer = ByteBuffer.allocate(buffer.capacity());

        FileUtils.writeFully(channel, buffer);
        FileUtils.readFully(channel, readBuffer, 0);

        assertEquals(TEST_CONTENT, new String(readBuffer.array()));
    }
}