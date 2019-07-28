package org.logdb.benchmark;

import org.logdb.LogDb;
import org.logdb.LogDbBuilder;
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
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static org.logdb.benchmark.DefaultBenchmarkConfig.BYTE_ORDER;
import static org.logdb.benchmark.DefaultBenchmarkConfig.PAGE_SIZE_BYTES;
import static org.logdb.benchmark.DefaultBenchmarkConfig.SEGMENT_FILE_SIZE;

public class TestRandomReadingBenchmark
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestRandomReadingBenchmark.class);

    @State(Scope.Benchmark)
    public static class BenchmarkState
    {
        static final int NUMBER_OF_PAIRS = 1_000_000;
        private Path rootDirectory;
        private LogDb logDb;
        private Random random;

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
                    .asyncIndexWrite(true)
                    .asyncQueueCapacity(16384)
                    .shouldSyncWrite(false)
                    .build();

            final byte[] valueBuffer = new byte[Long.BYTES];
            random = new Random();

            LOGGER.info("===================Creating Database...");

            for (int i = 0; i < NUMBER_OF_PAIRS; i++)
            {
                insert(i, i, valueBuffer);

                if (i % 100_000 == 0)
                {
                    LOGGER.info("===================Created 100_000 records...");
                    commit();
                }
            }

            LOGGER.info("===================Database Created");
            LOGGER.info("===================Starting Benchmark");
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws Exception
        {
            logDb.close();
            BenchmarkUtils.removeAllFilesFromDirectory(rootDirectory);
        }

        long getValue(final long key)
        {
            final byte[] bytes = logDb.get(key);
            return bytesToLong(bytes);
        }

        private void insert(final long key, final long value, final byte[] valueBuffer) throws IOException
        {
            longToBytes(value, valueBuffer);
            logDb.put(key, valueBuffer);
        }

        private long bytesToLong(final byte[] bytes)
        {
            return ((long) bytes[7] << 56) |
                    ((long) bytes[6] & 0xff) << 48 |
                    ((long) bytes[5] & 0xff) << 40 |
                    ((long) bytes[4] & 0xff) << 32 |
                    ((long) bytes[3] & 0xff) << 24 |
                    ((long) bytes[2] & 0xff) << 16 |
                    ((long) bytes[1] & 0xff) << 8 |
                    ((long) bytes[0] & 0xff);
        }


        private void longToBytes(final long value, final byte[] bytes)
        {
            bytes[0] = (byte) value;
            bytes[1] = (byte) (value >> 8);
            bytes[2] = (byte) (value >> 16);
            bytes[3] = (byte) (value >> 24);
            bytes[4] = (byte) (value >> 32);
            bytes[5] = (byte) (value >> 40);
            bytes[6] = (byte) (value >> 48);
            bytes[7] = (byte) (value >> 56);
        }

        private void commit()
        {
            try
            {
                logDb.commitIndex();
            }
            catch (final IOException e)
            {
                e.printStackTrace();
            }
        }

        long getRandomKey()
        {
            return random.nextInt(NUMBER_OF_PAIRS);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(5)
    public void testBench(final BenchmarkState benchmarkState, final Blackhole blackhole)
    {
        blackhole.consume(benchmarkState.getValue(benchmarkState.getRandomKey()));
    }
}
