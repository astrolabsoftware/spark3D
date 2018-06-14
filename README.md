![logo](https://github.com/JulienPeloton/spark3D/blob/master/pic/spark3d_logo.jpg)

[![Build Status](https://travis-ci.org/JulienPeloton/spark3D.svg?branch=master)](https://travis-ci.org/JulienPeloton/spark3D)
[![codecov](https://codecov.io/gh/JulienPeloton/spark3D/branch/master/graph/badge.svg)](https://codecov.io/gh/JulienPeloton/spark3D)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.JulienPeloton/spark3d_2.11/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.JulienPeloton/spark3d_2.11)

**The package is under an active development!**

## Latest News

- [05/2018] **GSoC 2018**: spark3D has been selected to the Google Summer of Code (GSoC) 2018. Congratulation to [@mayurdb](https://github.com/mayurdb) who will work on the project this year!
- [06/2018] **Release**: version 0.1.0, 0.1.1

## Why spark3D?

The volume of data recorded by current and
future High Energy Physics & Astrophysics experiments,
and the complexity of their data set require a broad panel of
knowledge in computer science, signal processing, statistics, and physics.
Exact analysis of those data sets produced is a serious computational challenge,
which cannot be done without the help of state-of-the-art tools.
This has to be matched by sophisticated and robust analysis performed on many
powerful machines, as we need to process or simulate several times data sets.

While a lot of efforts have been made to develop cluster computing systems for
processing large-scale spatial 2D data
(see e.g. [GeoSpark](http://geospark.datasyslab.org)),
there are very few frameworks to process and analyse 3D data sets
which were hitherto too costly to be processed efficiently.
With the recent development of fast and general engine such as
[Apache Spark](http://spark.apache.org), taking advantage of
distributed systems, we enter in a new area of possibilities.
[spark3D](https://github.com/JulienPeloton/spark3D) extends Apache Spark to
efficiently load, process, and analyze large-scale 3D spatial data sets across machines.

### Goals of spark3D


We have to distribute the computation for a very large amount of 3D data.
Several goals have to be undertaken in this project:

- construct a highly scalable architecture so as to accept very large dataset. (typically greater than what can be handled by practical memory sets)
- datasets are sets of 3D data, ie. object data containing both 3D information plus additional related data.
- mechanisms should offer:
  + indexing mechanisms
  + ways to define a metric (ie. distance between objects)
  + selection capability of objects or objects within a region
- work with as many input file format as possible (CSV, JSON, FITS, and so on)
- package the developments into an open-source library.

### Current spark3D package

The current spark3D package provides

- Read and format data from external data sets. Coordinates can be spherical or cartesian.
  - *Currently available: FITS and CSV data format.*
- Instantiate 3D objects.
  - *Currently available: Point, Sphere, Spherical shell, Box.*
- Create RDD[T] from a raw RDD whose T is a 3D object. The new RDD has the same partitioning as the raw RDD.
  - *Currently available: RDD[Point], RDD[Sphere]*
- Re-partition RDD[T].
  - *Currently available: Onion grid, Octree.*
- Identification and join methods between two data sets.
  - *Currently available: cross-match between two RDD.*

What we are thinking at

- RangeQuery and KNN methods?
- KB Tree?
- [Project issues](https://github.com/JulienPeloton/spark3D/issues)
- ?

## Requirements

This library requires Spark 2.0+ (not tested for earlier version). The
library has been tested with Scala 2.10.6 and 2.11.X. If you want to use
another version, feel free to contact us. In addition to Spark, the library has currently two external dependencies: [healpix](http://healpix.sourceforge.net/) and [spark-fits](https://github.com/JulienPeloton/spark-fits). Unfortunately, there are no Maven coordinates for healpix, so we release a jar of the latest version within spark3D under the `lib/` folder.

## Including spark3D in your project

You can link spark3D to your project (either `spark-shell` or `spark-submit`) by specifying the coordinates:

```console
toto:~$ spark-submit --packages "com.github.JulienPeloton:spark3d_2.11:0.1.1" <...>
```

It might not contain the latest features though (see *Building from source*).

## Building from source

If you want to contribute to the project, or have access to the latest features, you can fork and clone the project, and build it from source.
This library is easily built with SBT (see the `build.sbt` script provided). To
build a JAR file simply run

```console
toto:~$ sbt ++${SCALA_VERSION} package
```

from the project root. The build configuration includes support for
Scala 2.10.6 and 2.11.X. In addition you can build the doc using SBT:

```console
toto:~$ sbt ++${SCALA_VERSION} doc
toto:~$ open target/scala_${SCALA_VERSION}/api/index.html
```

You can also encapsulate the external dependencies in the spark3D jar by constructing a fat jar:

```console
toto:~$ sbt ++${SCALA_VERSION} assembly
```

By doing so, you will be able to use spark3D in your program without specifying its external dependencies. Note that since healpix has no Maven coordinates, you still have to specify the jar when running your application (see the `lib/` folder).

## Running the test suite

To launch the test suite, just execute:

```console
toto:~$ sbt ++${SCALA_VERSION} coverage test coverageReport
```

We also provide a script (test.sh) that you can execute. You should get the
result on the screen, plus details of the coverage at
`target/scala_${SCALA_VERSION}/scoverage-report/index.html`.

## Using with spark-shell

First produce a jar of the spark3D library, and then launch a spark-shell by specifying the external dependencies:

```console
toto:~$ JARS="target/scala-2.11/spark3d_2.11-0.1.1.jar,lib/jhealpix.jar"
toto:~$ PACKAGES="com.github.JulienPeloton:spark-fits_2.11:0.4.0"
toto:~$ spark-shell --jars $JARS --packages $PACKAGES
```

You will be able to import anything from spark3D

```scala
scala> import com.spark3d.geometryObjects.Point3D
scala> // etc...
```
Note that if you make a fat jar (that is building with `sbt assembly` and not `sbt package`), you do not need to specify external dependencies as they are already included in the resulting jar:

```console
toto:~$ FATJARS="target/scala-2.11/spark3D-assembly-0.1.1.jar"
toto:~$ spark-shell --jars $FATJARS
```

## Using with jupyter notebook and examples

We include a number of notebooks to describe the use of the library in the folder `examples/jupyter`. We included a [README](https://github.com/JulienPeloton/spark3D/blob/master/examples/jupyter/README.md) to install Apache Toree as kernel in Jupyter.

## Batch mode and provided examples

We include [examples](https://github.com/JulienPeloton/spark3D/tree/master/src/main/scala/com/spark3d/examples) and runners (`run_*.sh`) in the root folder.
You might have to modify those scripts with your environment.

## Data sources

Since the scientific domain considered here is mostly the Astrophysics domain,
the natural storage or exchange file format is the FITS format.
Therefore we consider as part of the problem, the possibility to allow FITS files
to be directly injected into the HDFS infrastructure, so as to develop a Spark based applications. The usual [cfitsio](https://heasarc.gsfc.nasa.gov/fitsio/fitsio.html) library, as well as the FITS I/O format are not adapted to a distributed file system as HDFS.
Therefore we will have to develop low level Reader/Writer services,
to support direct access to FITS data, without copy nor conversion needs.
To tackle this challenge, we started a new project called
[spark-fits](https://github.com/JulienPeloton/spark-fits), which provides a
Spark connector for FITS data, and a Scala library for manipulating FITS file.

The other input format available is CSV. We plan to release more in the future, and you are welcome to submit requests for specific data source!

## Scala and 3D Visualisation

For the moment, our visualisation is done thanks to [smile](https://github.com/haifengl/smile). You can have an overview in the `examples/jupyter` folder.

| Raw data set | Re-partitioned data set |
|:---------:|:---------:|
| ![raw](https://github.com/JulienPeloton/spark3D/blob/master/examples/jupyter/images/myOnionFigRaw.png) | ![repartitioning](https://github.com/JulienPeloton/spark3D/blob/master/examples/jupyter/images/myOnionFig.png)|

If you have better tools or want to develop something specific, let us know!

## Contributors

* Julien Peloton (peloton at lal.in2p3.fr)
* Christian Arnault (arnault at lal.in2p3.fr)
* Mayur Bhosale (mayurdb31 at gmail.com) -- GSoC 2018.

Contributing to spark3D: see [CONTRIBUTING](https://github.com/JulienPeloton/spark3D/blob/master/CONTRIBUTING.rst).
