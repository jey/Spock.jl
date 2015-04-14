# Spock

An interface to [Apache Spark](https://spark.apache.org) for the [Julia](http://www.julia-lang.org) language.

## Build Instructions

1. Create a symlink from your `spark-assembly` jar to `lib/spark.jar` (or copy it).

  ```
  mkdir lib
  ln -s ~/Downloads/spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3.0-hadoop2.4.0.jar lib/spark.jar
  ```

2. Type `make` to build `spock.jar` and run the tests.

[![Build Status](https://travis-ci.org/jey/Spock.jl.svg?branch=master)](https://travis-ci.org/jey/Spock.jl)
