package org.logdb.storage.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.function.Predicate;

public enum FileType implements Predicate<Path>, Comparator<Path>
{
    HEAP("heap.logdb"),
    INDEX("index.logdbIndex"),
    ROOT_INDEX("root.logdbIndex");

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

    @Override
    public int compare(final Path file1, final Path file2)
    {
        return (int) (FileType.getFileSequence(file1) - FileType.getFileSequence(file2));
    }
}
