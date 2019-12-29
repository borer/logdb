package org.logdb.benchmark;

import org.logdb.LogDb;
import org.logdb.bit.BinaryHelper;
import org.logdb.builder.LogDbBuilder;
import org.logdb.time.SystemTimeSource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static org.logdb.benchmark.DefaultBenchmarkConfig.BYTE_ORDER;
import static org.logdb.benchmark.DefaultBenchmarkConfig.PAGE_SIZE_BYTES;
import static org.logdb.benchmark.DefaultBenchmarkConfig.SEGMENT_FILE_SIZE;

public class TestRandomSyncWritingBenchmark
{
    @State(Scope.Benchmark)
    public static class BenchmarkState
    {
        static final int NUMBER_OF_PAIRS = 1_000_000;
        private Path rootDirectory;
        private LogDb logDb;
        private Random random;
        private byte[] longBuffer;

        @Setup(Level.Trial)
        public void doSetup() throws IOException
        {
            rootDirectory = Paths.get("./benchmark_root");
            Files.createDirectories(rootDirectory);

            logDb = new LogDbBuilder()
                    .setRootDirectory(rootDirectory)
                    .setByteOrder(BYTE_ORDER)
                    .setPageSizeBytes(PAGE_SIZE_BYTES)
                    .setSegmentFileSize(SEGMENT_FILE_SIZE)
                    .useIndexWithLog(true)
                    .setTimeSource(new SystemTimeSource())
                    .asyncIndexWrite(false)
                    .asyncQueueCapacity(16384)
                    .shouldSyncWrite(true)
                    .build();

            longBuffer = new byte[Long.BYTES];
            random = new Random();
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws Exception
        {
            logDb.close();
            BenchmarkUtils.removeAllFilesFromDirectory(rootDirectory);
        }

        void insertRandom()
        {
            final int randomNumber = random.nextInt(NUMBER_OF_PAIRS);
            BinaryHelper.longToBytes(randomNumber, longBuffer);
            try
            {
                logDb.put(longBuffer, longBuffer);
                logDb.commitIndex();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(1)
    public void testBench(final BenchmarkState benchmarkState)
    {
        benchmarkState.insertRandom();
    }
}
