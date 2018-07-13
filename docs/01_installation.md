---
permalink: /docs/installation/
layout: splash
title: "Tutorial: Installation"
date: 2018-06-15 22:31:13 +0200
redirect_from:
  - /theme-setup/
---

# Tutorial: Installation

## Requirements

This library requires Spark 2.0+ (not tested for earlier version). The
library has been tested with Scala 2.11. If you want to use
another version, feel free to contact us. In addition to Spark, the library has currently two external dependencies: [healpix](http://healpix.sourceforge.net/) and [spark-fits](https://github.com/astrolabsoftware/spark-fits). Unfortunately, there are no Maven coordinates for healpix, so we release a jar of the latest version within spark3D under the `lib/` folder.

## Including spark3D in your project

You can link spark3D to your project (either `spark-shell` or `spark-submit`) by specifying the coordinates:

```bash
toto:~$ spark-submit --packages "com.github.astrolabsoftware:spark3d_2.11:0.1.4" <...>
```

It might not contain the latest features though (see *Building from source*).
You can check the latest available version at the root of the project (see the maven central badge)

## Building from source

If you want to contribute to the project, or have access to the latest features, you can fork and clone the project, and build it from source.
This library is easily built with SBT (see the `build.sbt` script provided). To
build a JAR file simply run

```bash
toto:~$ sbt ++${SCALA_VERSION} package
```

from the project root. The build configuration includes support for
Scala 2.11. In addition you can build the doc using SBT:

```bash
toto:~$ sbt ++${SCALA_VERSION} doc
toto:~$ open target/scala_${SCALA_VERSION}/api/index.html
```

You can also encapsulate the external dependencies in the spark3D jar by constructing a fat jar:

```bash
toto:~$ sbt ++${SCALA_VERSION} assembly
```

By doing so, you will be able to use spark3D in your program without specifying its external dependencies. Note that since healpix has no Maven coordinates, you still have to specify the jar when running your application (see the `lib/` folder).

## Running the test suite

To launch the test suite, just execute:

```bash
toto:~$ sbt ++${SCALA_VERSION} coverage test coverageReport
```

We also provide a script (test.sh) that you can execute. You should get the
result on the screen, plus details of the coverage at
`target/scala_${SCALA_VERSION}/scoverage-report/index.html`.

## Using with spark-shell

First produce a jar of the spark3D library, and then launch a spark-shell by specifying the external dependencies:

```bash
toto:~$ JARS="target/scala-2.11/spark3d_2.11-0.1.4.jar,lib/jhealpix.jar"
toto:~$ PACKAGES="com.github.astrolabsoftware:spark-fits_2.11:0.6.0"
toto:~$ spark-shell --jars $JARS --packages $PACKAGES
```

You will be able to import anything from spark3D

```scala
scala> import com.astrolabsoftware.spark3d.geometryObjects.Point3D
scala> // etc...
```
Note that if you make a fat jar (that is building with `sbt assembly` and not `sbt package`), you do not need to specify external dependencies as they are already included in the resulting jar:

```bash
toto:~$ FATJARS="target/scala-2.11/spark3D-assembly-0.1.4.jar"
toto:~$ spark-shell --jars $FATJARS
```

## Using with jupyter notebook and examples

We include a number of notebooks to describe the use of the library in the folder `examples/jupyter`. We included a [README](https://github.com/astrolabsoftware/spark3D/blob/master/examples/jupyter/README.md) to install Apache Toree as kernel in Jupyter.

## Batch mode and provided examples

You can follow the different tutorials:
- [Loading data]({{ site.baseurl }}{% link 02_introduction.md %})
- [Space partitioning]({{ site.baseurl }}{% link 03_partitioning.md %})
- [Query]({{ site.baseurl }}{% link 04_query.md %})

We also include [examples](https://github.com/astrolabsoftware/spark3D/tree/master/src/main/scala/com/spark3d/examples) and runners (`run_*.sh`) in the root folder of the repo.
You might have to modify those scripts with your environment.
