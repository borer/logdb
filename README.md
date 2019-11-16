# LogDB

[![Build Status](https://api.travis-ci.org/borer/logdb.svg?branch=master)](https://travis-ci.org/borer/logdb)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/borer/logdb.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/borer/logdb/context:java)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/borer/logdb.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/borer/logdb/alerts/)

Log structured based database

To compile and run the the tests :

```sh
mvn clean
mvn package
```

To run the benchmark :

```sh
./runBenchmarks.sh
```


Benchmarks
---

System Info:

```
CPU : Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz
Memory : 64GB
Disk : Liteonit LCS-256L9S-11 2.5 7mm 256GB
OS: Fedora release 30 (Thirty)
```

- Read Benchmark : Reading of 8 byte values using 5 threads

```
Result "org.logdb.benchmark.TestRandomReadingBenchmark.testBench":
  2208715.702 ±(99.9%) 159660.336 ops/s [Average]
  (min, avg, max) = (1972713.020, 2208715.702, 2466341.321), stdev = 213141.981
  CI (99.9%): [2049055.366, 2368376.038] (assumes normal distribution)

Benchmark                              Mode  Cnt        Score        Error  Units
TestRandomReadingBenchmark.testBench  thrpt   25  2208715.702 ± 159660.336  ops/s
```

- Async Write Benchmark : Writing 16 bytes  bytes key/value pairs (avg 230 Mib/s)

```
Result "org.logdb.benchmark.TestRandomAsyncWritingBenchmark.testBench":
  44414.983 ±(99.9%) 2065.765 ops/s [Average]
  (min, avg, max) = (41840.835, 44414.983, 46270.895), stdev = 1366.376
  CI (99.9%): [42349.218, 46480.747] (assumes normal distribution)

Benchmark                                   Mode  Cnt      Score      Error  Units
TestRandomAsyncWritingBenchmark.testBench  thrpt   10  44414.983 ± 2065.765  ops/s
```

- Sync Write Benchmark : Writing 16 bytes key/value pairs using fully sync commits after every write (avg 6 Mib/s)

```
Result "org.logdb.benchmark.TestRandomSyncWritingBenchmark.testBench":
  117.990 ±(99.9%) 1.577 ops/s [Average]
  (min, avg, max) = (116.071, 117.990, 119.280), stdev = 1.043
  CI (99.9%): [116.412, 119.567] (assumes normal distribution)

Benchmark                                  Mode  Cnt    Score   Error  Units
TestRandomSyncWritingBenchmark.testBench  thrpt   10  117.990 ± 1.577  ops/s
```


License
----
Bogdan Gochev
MIT
