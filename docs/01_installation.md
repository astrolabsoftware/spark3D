---
permalink: /docs/installation/
layout: splash
title: "Tutorial: Installation"
date: 2018-06-15 22:31:13 +0200
redirect_from:
  - /theme-setup/
---

# Scala installation

## Requirements

This library requires Spark 2.0+ (not tested for earlier version). The
library has been tested with Scala 2.11. If you want to use
another version, feel free to contact us. In addition to Spark, the library has currently two external dependencies: [healpix](http://healpix.sourceforge.net/) and [spark-fits](https://github.com/astrolabsoftware/spark-fits). Unfortunately, there are no Maven coordinates for healpix, so we release a jar of the latest version within spark3D under the `lib/` folder.

## Including spark3D in your project

You can link spark3D to your project (either `spark-shell` or `spark-submit`) by specifying the coordinates:

```bash
toto:~$ spark-submit --packages "com.github.astrolabsoftware:spark3d_2.11:0.1.5" <...>
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
toto:~$ JARS="target/scala-2.11/spark3d_2.11-0.1.5.jar,lib/jhealpix.jar"
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
toto:~$ FATJARS="target/scala-2.11/spark3D-assembly-0.1.5.jar"
toto:~$ spark-shell --jars $FATJARS
```

## Using with jupyter notebook and examples

We include a number of notebooks to describe the use of the library in the folder `examples/jupyter`. We included a [README](https://github.com/astrolabsoftware/spark3D/blob/master/examples/jupyter/README.md) to install Apache Toree as kernel in Jupyter.

# Python installation

## pyspark3d: A Python interface to spark3D

pyspark3d is directly built around spark3D using [py4j](https://www.py4j.org/).
As a result, new features are first added in spark3D and then propagated
to pyspark3d (sometimes with delay). The basic idea is to load spark3D
objects in the Java Virtual Machine, and then Python programs running in a Python interpreter dynamically access Java objects in the JVM.

## Requirements

pyspark is tested on Python 3.5 and later.
**Note: pyspark3d will not run for python 3.4 and earlier (incl. 2.X).** The reason is
that we make use of [type hints](https://www.python.org/dev/peps/pep-0484/):
```
PEP 484, the typing module, a new standard for type annotations.
```

In addition, pyspark3d requires:

- `pyspark` (python)
- `spark-fits` (scala)

You can find all python dependencies in the `requirements.txt` file at the root of the project.

## Including spark3D in your project

In case you are completely lost, have a look at the `.travis.yml` file at the
root of the project. It will give you a hint on how to install the project
on ubuntu 16.04.

### Manual

In order to use pyspark3d, you first need to connect it to spark3D either by
specifying the Maven coordinates of the package (+ dependencies) or link to
an assembly JAR. For example, download
the latest version of spark3D (including pyspark3d), and build an assembly
JAR (see also `Building from source` above):

```bash
toto:~$ cd /path/to/spark3D
toto:~$ sbt ++${SCALA_VERSION} assembly
```

Edit the `pyspark3d_conf.py` with the newly created JAR:

```python
# in pyspark3d_conf.py

...

# spark3D version
version = __version__

# Scala version used to compile spark3D
scala_version = "2.11"

...

# External JARS to be added to both driver and executors
# Should contain the FAT JAR of spark3D.
extra_jars = [
    os.path.join(
        path_to_conf, "../target/scala-{}/spark3D-assembly-{}.jar".format(
            scala_version, version))
]
```

Make sure spark3D is also in your PYTHONPATH:

```bash
# in ~/.bash_profile for example
...
export PYTHONPATH=/path/to/spark3D:$PYTHONPATH
...
```

### Using pip

To come.

## Running the test suite

To launch the test suite, just execute the python script at the root of the package:

```bash
toto:~$ ./test_python ${SCALA_VERSION}
```

We use [doctest](https://docs.python.org/3/library/doctest.html). Note that it will build the JAR for you, so you need to have Scala and SBT installed on your machine. To printout the coverage report on screen, just modify the script with your environment:

```bash
...
isLocal=`whoami`
if [ $isLocal == <put_your_name> ]
then
 ...
fi
```

## Using with pyspark

First produce a FAT JAR of the spark3D library (see above), and then launch a shell for example by specifying the external dependencies:

```bash
toto:~$ PYSPARK_DRIVER_PYTHON=ipython pyspark \
  --jars /path/to/target/scala-2.11/spark3D-assembly-0.1.5.jar \
  --packages com.github.astrolabsoftware:spark-fits_2.11:0.6.0
```

You should be able to import objects:

```python
In [1]: import pyspark3d
In [2]: from pyspark3d.geometryObjects import Point3D
In [3]: Point3D?
Signature: Point3D(x:float, y:float, z:float, isSpherical:bool) -> py4j.java_gateway.JavaObject
Docstring:
Binding arount Point3D.scala. For full description,
see `$spark3d/src/main/scala/com/spark3d/geometryObjects/Point3D.scala`.

By default, the input coordinates are supposed euclidean,
that is (x, y, z). The user can also work with spherical input coordinates
(x=r, y=theta, z=phi) by setting the argument isSpherical=true.

Parameters
----------
x : float
    Input X coordinate in Euclidean space, and R in spherical space.
y : float
    Input Y coordinate in Euclidean space, and THETA in spherical space.
z : float
    Input Z coordinate in Euclidean space, and PHI in spherical space.
isSpherical : bool
    If true, it assumes that the coordinates of the Point3D
    are (r, theta, phi). Otherwise, it assumes cartesian
    coordinates (x, y, z).

Returns
----------
p3d : Point3D instance
    An instance of the class Point3D.

Example
----------
Instantiate a point with spherical coordinates (r, theta, phi)
>>> p3d = Point3D(1.0, np.pi, 0.0, True)

The returned type is JavaObject (Point3D instance)
>>> print(type(p3d))
<class 'py4j.java_gateway.JavaObject'>

You can then call the method associated, for example
>>> p3d.getVolume()
0.0

Convert the (theta, phi) in Healpix pixel index:
>>> p3d.toHealpix(2048, True)
50331644

To see all the available methods:
>>> print(sorted(p3d.__dir__())) # doctest: +NORMALIZE_WHITESPACE
['center', 'distanceTo', 'equals', 'getClass', 'getCoordinate',
'getEnvelope', 'getHash', 'getVolume', 'hasCenterCloseTo', 'hashCode',
'intersects', 'isEqual', 'isSpherical', 'notify', 'notifyAll', 'toHealpix',
'toHealpix$default$2', 'toString', 'wait', 'x', 'y', 'z']
File:      ~/Documents/workspace/myrepos/spark3D/pyspark3d/geometryObjects.py
Type:      function
```

# Batch mode and provided examples

You can follow the different tutorials:
- Loading data [Scala]({{ site.baseurl }}{% link 02_introduction_scala.md %}), [Python]({{ site.baseurl }}{% link 02_introduction_python.md %})
- Space partitioning [Scala]({{ site.baseurl }}{% link 03_partitioning_scala.md %}), [Python]({{ site.baseurl }}{% link 03_partitioning_python.md %})
- Query [Scala]({{ site.baseurl }}{% link 04_query_scala.md %}), [Python]({{ site.baseurl }}{% link 04_query_python.md %})

We also include [Scala examples](https://github.com/astrolabsoftware/spark3D/tree/master/src/main/scala/com/spark3d/examples) and runners (`run_*.sh`) in the root folder of the repo.
You might have to modify those scripts with your environment.
