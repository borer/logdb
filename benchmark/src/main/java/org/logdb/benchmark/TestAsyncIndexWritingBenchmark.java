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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.logdb.benchmark.DefaultBenchmarkConfig.BYTE_ORDER;
import static org.logdb.benchmark.DefaultBenchmarkConfig.PAGE_SIZE_BYTES;
import static org.logdb.benchmark.DefaultBenchmarkConfig.SEGMENT_FILE_SIZE;

public class TestAsyncIndexWritingBenchmark
{
    @State(Scope.Benchmark)
    public static class BenchmarkState
    {
        private long key;
        private long value;
        private byte[] valueBuffer;
        private Path rootDirectory;
        private LogDb logDb;

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
                    .build();

            key = 1L;
            value = 1L;
            valueBuffer = new byte[Long.BYTES];
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws Exception
        {
            logDb.close();
            BenchmarkUtils.removeAllFilesFromDirectory(rootDirectory);
        }

        void insert() throws IOException
        {
            longToBytes(value, valueBuffer);

            logDb.put(key, valueBuffer);

            ++key;
            ++value;

        }

        void longToBytes(final long value, final byte[] bytes)
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

        void commit()
        {
            try
            {
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
    public void testBench(final BenchmarkState benchmarkState) throws IOException
    {
        benchmarkState.insert();
        benchmarkState.commit();
    }
}
