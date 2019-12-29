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
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static java.lang.System.exit;
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
                    .shouldSyncWrite(false)
                    .build();

            final byte[] valueBuffer = new byte[Long.BYTES];
            final byte[] keyBuffer = new byte[Long.BYTES];
            random = new Random();
            longBuffer = new byte[Long.BYTES];

            LOGGER.info("===================Creating Database...");

            for (int i = 0; i < NUMBER_OF_PAIRS; i++)
            {
                insert(i, i, keyBuffer, valueBuffer);

                if (i % 100_000 == 0)
                {
                    LOGGER.info("===================Created 100_000 records...");
                    commit();
                }
            }

            commit();
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
            BinaryHelper.longToBytes(key, longBuffer);
            final byte[] value = logDb.get(longBuffer);
            return BinaryHelper.bytesToLong(value);
        }

        private void insert(final long key, final long value, final byte[] keyBuffer, final byte[] valueBuffer) throws IOException
        {
            BinaryHelper.longToBytes(key, keyBuffer);
            BinaryHelper.longToBytes(value, valueBuffer);
            logDb.put(keyBuffer, valueBuffer);
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
        final long randomKey = benchmarkState.getRandomKey();
        try
        {
            blackhole.consume(benchmarkState.getValue(randomKey));
        }
        catch (Exception e)
        {
            LOGGER.error("unable to read " + randomKey, e);
            exit(-1);
        }
    }
}
