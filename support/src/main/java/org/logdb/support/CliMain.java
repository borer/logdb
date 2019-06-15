package org.logdb.support;

import org.logdb.LogDb;
import org.logdb.LogDbBuilder;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.time.SystemTimeSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Scanner;

public class CliMain
{
    private static final @ByteSize int PAGE_SIZE_BYTES = StorageUnits.size(4096);
    private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final @ByteSize int MEMORY_MAPPED_CHUNK_SIZE_BYTES = StorageUnits.size(819200);
    private static final String EXIT_COMMAND = "exit";
    private static final String HELP_COMMAND = "help";
    private static final String GET_COMMAND = "get";
    private static final String PUT_COMMAND = "put";
    private static final String DELETE_COMMAND = "delete";

    public static void main(String[] args)
    {
        if (args.length < 2)
        {
            System.out.println("Usage : <root directory> <log db name>");
            return;
        }

        final String rootDirectoryArg = args[0];
        final File rootDirectory = new File(rootDirectoryArg);
        if (!rootDirectory.exists() || !rootDirectory.isDirectory())
        {
            System.out.println("Root directory " + rootDirectoryArg + " doesn't exists or is not a directory");
            return;
        }

        final String dbNameArg = args[1];
        final File dbfile = new File(rootDirectory, dbNameArg);
        if (!isFilenameValid(dbfile))
        {
            System.out.println("Db file name " + dbNameArg + " is not a valid file name");
            return;
        }

        final LogDbBuilder logDbBuilder = new LogDbBuilder();
        try (LogDb logDb = logDbBuilder
                                    .setRootDirectory(rootDirectory.getAbsolutePath())
                                    .setDbName(dbfile.getName())
                                    .setTimeSource(new SystemTimeSource())
                                    .setPageSizeBytes(PAGE_SIZE_BYTES)
                                    .setByteOrder(BYTE_ORDER)
                                    .setMemoryMappedChunkSizeBytes(MEMORY_MAPPED_CHUNK_SIZE_BYTES)
                                    .build())
        {
            final Scanner scanner = new Scanner(System.in);
            boolean terminate = false;
            while (!terminate)
            {
                System.out.print(">");
                final String command = scanner.nextLine();

                if (EXIT_COMMAND.equals(command))
                {
                    terminate = true;
                }
                else if (HELP_COMMAND.equals(command) || command.isEmpty())
                {
                    System.out.println("Possible commands are :");
                    System.out.println("\tget <key>");
                    System.out.println("\tput <key> <value>");
                    System.out.println("\tdelete <key>");
                }
                else
                {
                    final String[] commandParts = command.split(" ");
                    final String instruction = commandParts[0];
                    switch (instruction)
                    {
                        case GET_COMMAND:
                            handleGetCommand(logDb, command, commandParts);
                            break;
                        case PUT_COMMAND:
                            handlePutCommand(logDb, command, commandParts);
                            break;
                        case DELETE_COMMAND:
                            handleDeleteCommand(logDb, command, commandParts);
                            break;
                        default:
                            System.out.println("Command " + command + " is not a valid one. Try help for valid commands");
                            break;
                    }
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void handleDeleteCommand(final LogDb logDb, final String command, final String[] commandParts)
    {
        if (commandParts.length != 2)
        {
            System.out.println("Invalid instruction : " + command);
            return;
        }
        final long keyDelete = Long.parseLong(commandParts[1]);
        logDb.delete(keyDelete);
        System.out.println("Ok.");
    }

    private static void handlePutCommand(final LogDb logDb, final String command, final String[] commandParts)
    {
        if (commandParts.length != 3)
        {
            System.out.println("Invalid instruction : " + command);
            return;
        }
        final long keyPut = Long.parseLong(commandParts[1]);
        final byte[] valuePut = commandParts[2].getBytes();
        logDb.put(keyPut, valuePut);
        logDb.commitIndex();
        System.out.println("Ok.");
    }

    private static void handleGetCommand(final LogDb logDb, final String command, final String[] commandParts)
    {
        if (commandParts.length != 2)
        {
            System.out.println("Invalid instruction : " + command);
            return;
        }
        final long keyGet = Long.parseLong(commandParts[1]);
        final byte[] value = logDb.get(keyGet);
        if (value == null)
        {
            System.out.println("Key " + keyGet + " not found.");
        }
        else
        {
            System.out.println(new String(value));
        }
    }

    private static boolean isFilenameValid(final File file)
    {
        try
        {
            file.getCanonicalPath();
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}
