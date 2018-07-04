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
import com.spark3d.geometryObjects.Point3D

// Cartesian coordinates
val points = new Point3D(x: Double, y: Double, z: Double, isSpherical: Boolean = false)

// Spherical coordinates
val points = new Point3D(r: Double, theta: Double, phi: Double, isSpherical: Boolean = true)
```

### Shells and Spheres

```scala
import com.spark3d.geometryObjects.ShellEnvelope

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
import com.spark3d.geometryObjects.BoxEnvelope

// Box from region defined by three (cartesian) coordinates.
val boxes = new BoxEnvelope(p1: Point3D, p2: Point3D, p3: Point3D)

// Box from region defined by two (cartesian) coordinates.
val boxes = new BoxEnvelope(p1: Point3D, p2: Point3D)

// Box from region defined by one (cartesian) coordinates.
// The cube Envelope in this case will be a point.
val boxes = new BoxEnvelope(p1: Point3D)
```

## Supported data sources

One of the goal of spark3D is to support as many data source as possible. Currently, we focused our effort on: FITS, CSV, JSON, and TXT. While the first one is widely used in the Astrophysics community, the others are widely used in the industry.

In this tutorial we will review the steps to simply create RDD from 3D data sets. A 3DRDD is simply a RDD whose elements are 3D objects. Currently, spark3D supports 2 kind of objects: points (`Point3D`) and spheres (`ShellEnvelope`). Note that spheres are a sub-case of shells.

### Loading Point3D

A point is an object with 3 spatial coordinates. In spark3D, you can choose the coordinate system between cartesian `(x, y, z)` and spherical `(r, theta, phi)`. Let's suppose we have a text file (CSV, JSON, or TXT) whose columns are labeled `x`, `y` and `z`, the cartesian coordinates of points:

```scala
import com.spark3d.spatial3DRDD.Point3DRDD

// We assume filename contains at least 3 columns whose names are `colnames`
// Order of columns in the file does not matter, as they will be re-aranged
// according to `colnames`.
val pointRDD = new Point3DRDD(spark: SparkSession, filename: String, colnames: String, isSpherical: Boolean)
```

With FITS data, with data in the HDU #1, you would just do

```scala
import com.spark3d.spatial3DRDD.Point3DRDD

// We assume hdu#1 of filename contains at least 3 columns whose names are `colnames`
// Order of columns in the file does not matter, as they will be re-aranged
// according to `colnames`.
val hdu = 1
val pointRDD = new Point3DRDD(spark: SparkSession, filename: String, hdu: Int, colnames: String, isSpherical: Boolean)
```

The resulting RDD is a `RDD[Point3D]`. Note that there is no space between columns labels.

### Loading Sphere

A sphere is defined by its center (3 spatial coordinates) plus a radius.
In spark3D, you can choose the coordinate system of the center between cartesian `(x, y, z)` and spherical `(r, theta, phi)`. Let's suppose we have a text file (CSV, JSON, or TXT) whose columns are labeled `r`, `theta`, `phi`, the spherical coordinates and `radius`:

```scala
import com.spark3d.spatial3DRDD.SphereRDD

// We assume filename contains at least 4 columns whose names are `colnames`.
// Order of columns in the file does not matter, as they will be re-aranged
// according to `colnames`.
val pointRDD = new SphereRDD(spark: SparkSession, filename: String, colnames: String, isSpherical: Boolean)
```

The resulting RDD is a `RDD[Sphere]`.

### Loading Shells and Boxes

TBD.

### Loading data from a different data source

Since the scientific domain considered here is mostly the Astrophysics domain,
the natural storage or exchange file format is the FITS format.
Therefore we consider as part of the problem, the possibility to allow FITS files
to be directly injected into the HDFS infrastructure, so as to develop a Spark based applications. The usual [cfitsio](https://heasarc.gsfc.nasa.gov/fitsio/fitsio.html) library, as well as the FITS I/O format are not adapted to a distributed file system as HDFS.
Therefore we will have to develop low level Reader/Writer services,
to support direct access to FITS data, without copy nor conversion needs.
To tackle this challenge, we started a new project called
[spark-fits](https://github.com/theastrolab/spark-fits), which provides a
Spark connector for FITS data, and a Scala library for manipulating FITS file.

We plan to release more in the future (HDF5 and ROOT on the TODO list!), and you are welcome to submit requests for specific data sources!
Alternatively you can define your own routine to read it. Add a new routine in `Loader.scala`:

```scala
/**
  * Your doc is important
  */
def Point3DRDDFromMySource(spark : SparkSession, filename : String, colnames : String,
    isSpherical: Boolean, <other_options>): RDD[Point3D] {

  // Read the data using your data source
  val df = spark.read.option(...)

  // Grab the name of columns
  val csplit = colnames.split(",")

  // Select the 3 columns (x, y, z)
  // and cast to double in case.
  val rawRDD = df.select(
    col(csplit(0)).cast("double"),
    col(csplit(1)).cast("double"),
    col(csplit(2)).cast("double")
  )
  // DF to RDD
  .rdd
  // map to Point3D
  .map(x => new Point3D(
    x.getDouble(0), x.getDouble(1), x.getDouble(2), isSpherical)
  )

  rawRDD
}

```

and then update `Point3DRDD.scala`:

```scala
/**
  * Your doc is important
  */
def this(spark : SparkSession, filename : String, colnames : String, isSpherical: Boolean, <other_options>) {
  this(Point3DRDDFromMySource(spark, filename, colnames, isSpherical, <other_options>), isSpherical)
}
```

and finally load your data using:

```scala
val myPoints = new Point3DRDD(spark : SparkSession, filename : String, colnames : String, isSpherical: Boolean, <other_options>)
```

here the example makes use of the DataFrame API, but you can use the RDD API as well to read your data.
