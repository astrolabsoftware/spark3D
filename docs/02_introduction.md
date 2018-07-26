---
permalink: /docs/introduction/
layout: splash
title: "Tutorial: Introduction"
date: 2018-06-15 22:31:13 +0200
---

# Tutorial: Introduction

## Manipulate 3D Shapes with spark3D

spark3D supports various 3D shapes: points (`Point3D`), spherical shells (`ShellEnvelope`, which includes sphere as well), and boxes (`BoxEnvelope`). You can easily create instances of those objects in spark3D, and they come with many methods (distance to, intersection/coverage/contain, volume, equality check, ...).

### Point3D

```scala
import com.astrolabsoftware.spark3d.geometryObjects.Point3D

// Cartesian coordinates
val points = new Point3D(x: Double, y: Double, z: Double, isSpherical: Boolean = false)

// Spherical coordinates
val points = new Point3D(r: Double, theta: Double, phi: Double, isSpherical: Boolean = true)
```

### Shells and Spheres

```scala
import com.astrolabsoftware.spark3d.geometryObjects.ShellEnvelope

// Shell from 3D coordinates + inner/outer radii
val shells = new ShellEnvelope(x: Double, y: Double, z: Double, isSpherical: Boolean, innerRadius: Double, outerRadius: Double)

// Shell from Point3D + inner/outer radii
val shells = new ShellEnvelope(center: Point3D, isSpherical: Boolean, innerRadius: Double, outerRadius: Double)

// Sphere from 3D coordinates + radius
val spheres = new ShellEnvelope(x: Double, y: Double, z: Double, isSpherical: Boolean, radius: Double)

// Sphere from Point3D + inner/outer radii
val spheres = new ShellEnvelope(center: Point3D, isSpherical: Boolean, radius: Double)
```

### Boxes

```scala
import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope

// Box from region defined by three (cartesian) coordinates.
val boxes = new BoxEnvelope(p1: Point3D, p2: Point3D, p3: Point3D)

// Box from region defined by two (cartesian) coordinates.
val boxes = new BoxEnvelope(p1: Point3D, p2: Point3D)

// Box from region defined by one (cartesian) coordinates.
// The cube Envelope in this case will be a point.
val boxes = new BoxEnvelope(p1: Point3D)
```

## Supported data sources

One of the goal of spark3D is to support as many data source as possible. Currently, you can load all Spark DataSource V2! That means CSV, JSON, parquet, Avro, ... In addition, you can load scientific data formats following Spark DataSource API like [FITS](https://github.com/astrolabsoftware/spark-fits), [ROOT](https://github.com/diana-hep/spark-root) (<= 6.11) or [HDF5](https://github.com/LLNL/spark-hdf5)!

In this tutorial we will review the steps to simply create RDD from 3D data sets. A 3DRDD is simply a RDD whose elements are 3D objects. Currently, spark3D supports 2 kind of objects: points (`Point3D`) and spheres (`ShellEnvelope`). Note that spheres are a sub-case of shells.

### Loading Point3D

A point is an object with 3 spatial coordinates. In spark3D, you can choose the coordinate system between cartesian `(x, y, z)` and spherical `(r, theta, phi)`. Let's suppose we have a file which contains data vectors labeled `x`, `y` and `z`, the cartesian coordinates of points:

```scala
import com.astrolabsoftware.spark3d.spatial3DRDD.Point3DRDD

// We assume filename contains at least 3 columns whose names are `colnames`
// Order of columns in the file does not matter, as they will be re-aranged
// according to `colnames`.
val pointRDD = new Point3DRDD(spark: SparkSession, filename: String, colnames: String,
  isSpherical: Boolean, format: String, options: Map[String, String],
  storageLevel: StorageLevel = StorageLevel.NONE)
```

Since we typically need to operate multiple times on the rawRDD (e.g. gather statistics), you can control the persistence on the rawRDD (default is `StorageLevel.NONE`, but you are strongly advised to choose `StorageLevel.MEMORY_ONLY` aka `.cache()` if you have enough memory) to reduce the execution time.
`format` and `options` control the correct reading of your data.

* `format` is the name of the data source as registered in Spark. For example: `csv`, `json`, `org.dianahep.sparkroot`, ... For Spark built-in see [here](https://github.com/apache/spark/blob/301bff70637983426d76b106b7c659c1f28ed7bf/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala#L560).
* `options`: Options to pass to the `DataFrameReader` (see below for examples).

**CSV / JSON**
```scala
// Spark datasource / You would replace "csv" by "json" for a json file
val format = "csv"
// Options to pass to the DataFrameReader - optional
val options = Map("header" -> "true")
```

**TXT**
```scala
// Spark datasource / use csv for text file with custom separator
val format = "csv"
// Options to pass to the DataFrameReader - optional
val options = Map("header" -> "true", "sep" -> " ")
```

**FITS**
```scala
// Spark datasource
val format = "fits" // or "com.astrolabsoftware.sparkfits"
// Options to pass to the DataFrameReader - optional
val options = Map("hdu" -> "1")
```

**HDF5**
```scala
// Spark datasource
val format = "hdf5" // or "gov.llnl.spark.hdf"
// Options to pass to the DataFrameReader - optional
val options = Map("dataset" -> "/toto")
```

**ROOT (<= 6.11)**
```scala
// Spark datasource
val format = "org.dianahep.sparkroot"
// Options to pass to the DataFrameReader - optional
val options = Map("" -> "")
```

The resulting RDD is a `RDD[Point3D]`. Note that there is no space between columns labels.

### Loading Sphere

A sphere is defined by its center (3 spatial coordinates) plus a radius.
In spark3D, you can choose the coordinate system of the center between cartesian `(x, y, z)` and spherical `(r, theta, phi)`. Let's suppose we have a file which contains data vectors labeled `r`, `theta`, `phi`, the spherical coordinates and `radius`. Similarly to `Point3DRDD` you would use:

```scala
import com.astrolabsoftware.spark3d.spatial3DRDD.SphereRDD

// We assume filename contains at least 4 columns whose names are `colnames`.
// Order of columns in the file does not matter, as they will be re-aranged
// according to `colnames`.
val sphereRDD = new SphereRDD(spark: SparkSession, filename: String, colnames: String,
  isSpherical: Boolean, format: String, options: Map[String, String],
  storageLevel: StorageLevel = StorageLevel.NONE)
```

The resulting RDD is a `RDD[ShellEnvelope]`.

### Loading Shells and Boxes

TBD.
