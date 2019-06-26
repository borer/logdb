package org.logdb.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;

public enum FileType implements Predicate<Path>
{
    HEAP("heap.logdb"),
    INDEX("index.logIndex"),
    ROOT_INDEX("root.logIndex");

    private final String fileNameSuffix;

    FileType(final String fileNameSuffix)
    {

        this.fileNameSuffix = fileNameSuffix;
    }

    String generateFilename(final long sequence)
    {
        return String.format("%d-" + fileNameSuffix, sequence);
    }

    @Override
    public boolean test(final Path file)
    {
        return Files.isRegularFile(file) && file.toString().endsWith(fileNameSuffix);
    }

    public static long getFileSequence(final Path filePath)
    {
        return Long.valueOf(filePath.getFileName().toString().split("-")[0]);
    }
}
