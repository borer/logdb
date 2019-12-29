package org.logdb.support;

import org.logdb.LogDb;
import org.logdb.bit.BinaryHelper;
import org.logdb.builder.LogDbBuilder;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.time.SystemTimeSource;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class CliMain
{
    private static final @ByteSize int PAGE_SIZE_BYTES = StorageUnits.size(4096);
    private static final @ByteSize int PAGE_LOG_SIZE_BYTES = StorageUnits.size(512);
    private static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;
    private static final @ByteSize int FILE_SEGMENT_SIZE = StorageUnits.size(819200);
    private static final String EXIT_COMMAND = "exit";
    private static final String HELP_COMMAND = "help";
    private static final String GET_COMMAND = "get";
    private static final String PUT_COMMAND = "put";
    private static final String DELETE_COMMAND = "delete";

    public static void main(String[] args)
    {
        if (args.length < 1)
        {
            System.out.println("Usage : <root directory>");
            return;
        }

        final String rootDirectoryArg = args[0];
        final Path rootDirectory = Paths.get(rootDirectoryArg);
        if (!Files.exists(rootDirectory) || !Files.isDirectory(rootDirectory))
        {
            System.out.println("Root directory " + rootDirectoryArg + " doesn't exists or is not a directory");
            return;
        }

        final LogDbBuilder logDbBuilder = new LogDbBuilder();
        try (LogDb logDb = logDbBuilder
                                    .setRootDirectory(rootDirectory)
                                    .setTimeSource(new SystemTimeSource())
                                    .setPageSizeBytes(PAGE_SIZE_BYTES)
                                    .setByteOrder(BYTE_ORDER)
                                    .setSegmentFileSize(FILE_SEGMENT_SIZE)
                                    .useIndexWithLog(true)
                                    .pageLogSize(PAGE_LOG_SIZE_BYTES)
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

    private static void handleDeleteCommand(final LogDb logDb, final String command, final String[] commandParts) throws IOException
    {
        if (commandParts.length != 2)
        {
            System.out.println("Invalid instruction : " + command);
            return;
        }
        final byte[] keyDelete = parseKey(commandParts[1]);
        logDb.delete(keyDelete);
        System.out.println("Ok.");
    }

    private static void handlePutCommand(final LogDb logDb, final String command, final String[] commandParts) throws IOException
    {
        if (commandParts.length != 3)
        {
            System.out.println("Invalid instruction : " + command);
            return;
        }
        final byte[] keyPut = parseKey(commandParts[1]);
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
        final String commandArgument = commandParts[1];
        final byte[] keyGet = parseKey(commandArgument);
        final byte[] value = logDb.get(keyGet);
        if (value == null)
        {
            System.out.println("Key " + commandArgument + " not found.");
        }
        else
        {
            System.out.println(new String(value));
        }
    }

    private static byte[] parseKey(String commandPart)
    {
        try
        {
            return BinaryHelper.longToBytes(Long.parseLong(commandPart));
        }
        catch (final NumberFormatException e)
        {
            return commandPart.getBytes();
        }
    }
}
