---
permalink: /docs/repartitioning/python/
layout: splash
title: "Tutorial: Introduction"
date: 2018-06-15 22:31:13 +0200
---

# Tutorial: Introduction

## Manipulate 3D Shapes with pyspark3d

spark3D supports various 3D shapes: points (`Point3D`), spherical shells (`ShellEnvelope`, which includes sphere as well), and boxes (`BoxEnvelope`). You can easily create instances of those objects in pyspark3d, and they come with many methods (distance to, intersection/coverage/contain, volume, equality check, ...).

### Point3D

```python
from pyspark3d.geometryObjects import Point3D

# Cartesian coordinates
points = Point3D(
	x: float, y: float, z: float, isSpherical: bool = False) -> JavaObject:

# Cartesian coordinates
points = Point3D(
	r: float, theta: float, phi: float, isSpherical: bool = True) -> JavaObject:
```

### Shells and Spheres

The Scala version makes use of several constructors (i.e. with different
kinds of argument). In order to mimick this in Python within a single routine, we
abstract the arguments of the constructor using the iterable `*args`.
There are then 5 possibilities to instantiate a `ShellEnvelope`:

- Case 1: Defined with a center coordinates, inner and outer radius.
- Case 2: Defined with a center coordinates, and a radius (= a sphere).
- Case 3: Defined with a Point3D, and a radius (= a sphere).
- Case 4: from another ShellEnvelope
- Case 5: Null envelope

```python
from pyspark3d.geometryObjects import ShellEnvelope

# Generic constructor
shells = ShellEnvelope(*args) -> JavaObject:

# Shell from 3D coordinates + inner/outer radii
shell = ShellEnvelope(
	x: float, y: float, z: float, isSpherical: bool,
	innerRadius: float, outerRadius: float) -> JavaObject:

# Shell from Point3D + inner/outer radii
shell = ShellEnvelope(
	center: Point3D, isSpherical: bool,
	innerRadius: float, outerRadius: float) -> JavaObject:

# Sphere from 3D coordinates + radius
sphere = ShellEnvelope(
	x: float, y: float, z: float, isSpherical: bool,
	radius: float) -> JavaObject:

# Sphere from Point3D + radius
sphere = ShellEnvelope(
	center: Point3D, isSpherical: bool, radius: float) -> JavaObject:
```

### Boxes

The Scala version makes use of several constructors (i.e. with different
kinds of argument). In order to mimick this in Python within a single routine, we
abstract the arguments of the constructor using the iterable `*args`.
There are then 5 possibilities to instantiate a `BoxEnvelope`:

- Case 1: from coordinates
- Case 2: from a single Point3D (i.e. the box is a Point3D)
- Case 3: from three Point3D
- Case 4: from another BoxEnvelope
- Case 5: Null envelope

**/!\ Coordinates of input Point3D MUST be cartesian.**

```python
from pyspark3d.geometryObjects import BoxEnvelope

# Box from region defined by three pairs of cartesian coordinates.
box = BoxEnvelope(
	xmin: float, xmax: float,
	ymin: float, ymax: float,
	zmin: float, zmax: float) : JavaObject

# Box from region defined by three (cartesian) coordinates.
box = BoxEnvelope(p1: Point3D, p2: Point3D, p3: Point3D) -> JavaObject:

# Box from region defined by two (cartesian) coordinates.
box = BoxEnvelope(p1: Point3D, p2: Point3D) -> JavaObject:

# Box from region defined by one (cartesian) coordinates.
# The cube Envelope in this case will be a point.
box = BoxEnvelope(p1: Point3D) -> JavaObject:
```

## Supported data sources

One of the goal of spark3D is to support as many data source as possible. Currently, you can load all Spark DataSource V2! That means CSV, JSON, parquet, Avro, ... In addition, you can load scientific data formats following Spark DataSource API like [FITS](https:#github.com/astrolabsoftware/spark-fits), [ROOT](https:#github.com/diana-hep/spark-root) (<= 6.11) or [HDF5](https:#github.com/LLNL/spark-hdf5)!

In this tutorial we will review the steps to simply create RDD from 3D data sets. A 3DRDD is simply a RDD whose elements are 3D objects. Currently, pyspark3d supports 2 kind of objects: points (`Point3D`) and spheres (`ShellEnvelope`). Note that spheres are a sub-case of shells.

### Loading Point3D

Note:
The Scala version makes use of several constructors (i.e. with different
kinds of argument). Here we only provide one way to instantiate a
Point3DRDD Scala class, through the full list of arguments.

Note that pyspark works with Python wrappers around the *Java* version
of Spark objects, not around the *Scala* version of Spark objects.
Therefore on the Scala side, we trigger the method
`Point3DRDDFromV2PythonHelper` which is a modified version of
`Point3DRDDFromV2`. The main change is that `options` on the Scala side
is a java.util.HashMap in order to smoothly connect to `dictionary` in
the Python side.

A point is an object with 3 spatial coordinates. In pyspark3d, you can choose the coordinate system between cartesian `(x, y, z)` and spherical `(r, theta, phi)`. Let's suppose we have a file which contains data vectors labeled `x`, `y` and `z`, the cartesian coordinates of points:

```python
from pyspark3d.spatial3DRDD import Point3DRDD

rdd = Point3DRDD(
	spark: SparkSession, filename: str, colnames: str,
	isSpherical: bool, format: str, options: Dict={"": ""})
```

For convenience (for the developper!), the StorageLevel is no more an
argument in the constructor (as for Scala) but is set to `StorageLevel.MEMORY_ONLY`.
This is because I couldn't find how to pass that information from
Python to Java... TBD!

`format` and `options` control the correct reading of your data.

* `format` is the name of the data source as registered in Spark. For example: `csv`, `json`, `org.dianahep.sparkroot`, ... For Spark built-in see [here](https:#github.com/apache/spark/blob/301bff70637983426d76b106b7c659c1f28ed7bf/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala#L560).
* `options`: Options to pass to the `DataFrameReader` (see below for examples).

**CSV / JSON**

```python
# Spark datasource / You would replace "csv" by "json" for a json file
format = "csv"
# Options to pass to the DataFrameReader - optional
options = {"header": "true"}
```

**TXT**

```python
# Spark datasource / use csv for text file with custom separator
format = "csv"
# Options to pass to the DataFrameReader - optional
options = {"header": "true", "sep": " "}
```

**FITS**

```python
# Spark datasource
format = "fits" # or "com.astrolabsoftware.sparkfits"
# Options to pass to the DataFrameReader - optional
options = {"hdu": "1"}
```

**HDF5**

```python
# Spark datasource
format = "hdf5" # or "gov.llnl.spark.hdf"
# Options to pass to the DataFrameReader - optional
options = {"dataset": "/toto"}
```

**ROOT (<= 6.11)**

```python
# Spark datasource
format = "org.dianahep.sparkroot"
# Options to pass to the DataFrameReader - optional
options = {"": ""}
```

The resulting RDD is a `RDD[Point3D]`.

### Loading Sphere

A sphere is defined by its center (3 spatial coordinates) plus a radius.
In spark3D, you can choose the coordinate system of the center between cartesian `(x, y, z)` and spherical `(r, theta, phi)`. Let's suppose we have a file which contains data vectors labeled `r`, `theta`, `phi`, the spherical coordinates and `radius`. Similarly to `Point3DRDD` you would use:

```python
from pyspark3d.spatial3DRDD import SphereRDD

sphereRDD = SphereRDD(
	spark: SparkSession, filename: str, colnames: str,
	isSpherical: bool, format: str, options: Dict={"": ""})
```

The resulting RDD is a `RDD[ShellEnvelope]`.

### Loading Shells and Boxes

TBD.
