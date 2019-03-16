# LogDB

[![Build Status](https://api.travis-ci.org/borer/logdb.svg?branch=master)](https://travis-ci.org/borer/logdb)

Log structured based database

To compile and run the the tests :

```sh
mvn clean
mvn package
```

To run the benchmark :

```sh
java -jar benchmark/target/benchmarks.jar TestBenchmark
```

*Note: when running the benchmark you need to give it a bit of heap memory depending on how fast 
it's running on your machine*

License
----
Bogdan Gochev
MIT
