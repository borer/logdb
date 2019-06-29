package org.logdb.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

final class FileAllocator
{
    private static final long INITIAL_SEQUENCE = 0L;

    private final Path rootDirectory;
    private long nextFileSequence;
    private final FileType fileType;

    private FileAllocator(
            final Path rootDirectory,
            final long nextFileSequence,
            final FileType fileType)
    {
        this.rootDirectory = rootDirectory;
        this.nextFileSequence = nextFileSequence;
        this.fileType = fileType;
    }

    static FileAllocator createNew(final Path rootDirectory, final FileType fileType)
    {
        return new FileAllocator(rootDirectory, INITIAL_SEQUENCE, fileType);
    }

    static FileAllocator openLatest(
            final Path rootDirectory,
            final Path lastPath,
            final FileType fileType)
    {
        final long nextFileSequence = FileType.getFileSequence(lastPath) + 1;
        return new FileAllocator(rootDirectory, nextFileSequence, fileType);
    }

    static Path findLastFile(final Path rootDirectory, final FileType fileType) throws IOException
    {
        final Optional<Path> maybeLastPath = Files.list(rootDirectory)
                .filter(fileType)
                .max(fileType);

        return maybeLastPath.orElseThrow(() -> new FileNotFoundException("cannot find any file of type " +
                fileType.name() + " in directory " + rootDirectory.toAbsolutePath().toString()));
    }

    List<Path> getAllFilesInOrder() throws IOException
    {
        return Files.list(rootDirectory)
                .filter(fileType)
                .sorted(fileType)
                .collect(Collectors.toList());
    }

    File generateNextFile()
    {
        final String filename = fileType.generateFilename(nextFileSequence);
        nextFileSequence++;

        return new File(rootDirectory.toFile(), filename);
    }
}
