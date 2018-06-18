---
permalink: /docs/introduction/
layout: splash
title: "Tutorial: Introduction"
date: 2018-06-15 22:31:13 +0200
---

# Tutorial: Introduction

## Loading data with spark3D

In this tutorial we will review the steps to simply create RDD from 3D data sets. A 3DRDD is simply a RDD whose elements are 3D objects. Currently, spark3D supports 3 kind of objects: points (`Point3D`), spherical shells (`ShellEnvelope`), and boxes (`BoxEnvelope`). Note that spheres are a sub-case of shells.

### Supported data sources

One of the goal of spark3D is to support as many data source as possible. Currently, we focused our effort on two: FITS and CSV. While the former is widely used in the Astrophysics community, the latter is widely used in the industry. We will see later in this tutorial how to load data from a different data source.

### Point3D

A point is an object with 3 spatial coordinates. In spark3D, you can choose the coordinate system between cartesian `(x, y, z)` and spherical `(r, theta, phi)`. Let's suppose we have a CSV file whose columns are labeled `x`, `y` and `z`, the cartesian coordinates of points:

```scala
import com.spark3d.spatial3DRDD.Point3DRDDFromCSV

val pointRDD = new Point3DRDDFromCSV(spark, "myfile.csv", "x,y,z", isSpherical=false)
```

The resulting RDD is a `RDD[Point3D]`.

### Sphere

A sphere is defined by its center (3 spatial coordinates) plus a radius.
In spark3D, you can choose the coordinate system of the center between cartesian `(x, y, z)` and spherical `(r, theta, phi)`. Let's suppose we have a CSV file whose columns are labeled `r`, `theta`, `phi`, the spherical coordinates and `radius`:

```scala
import com.spark3d.spatial3DRDD.SphereRDDFromCSV

val pointRDD = new SphereRDDFromCSV(spark, "myfile.csv", "r,theta,phi,radius", isSpherical=false)
```

The resulting RDD is a `RDD[Sphere]`.

### Shell

A shell is the difference an object with a center (3 spatial coordinates) plus an inner radius and an outer radius. TBD.


### Loading data from a different data source

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
Alternatively you can define your own routine to read it, taking inspiration from existing scripts:

```scala
class Point3DRDDFromMySource(spark : SparkSession, filename : String, colnames : String,
    override val isSpherical: Boolean) extends Shape3DRDD[Point3D] {

  // Read the data using your data source
  val df = spark.read.option(...)

  // Grab the name of columns
  val csplit = colnames.split(",")

  // Select the 3 columns (x, y, z)
  // and cast to double in case.
  override val rawRDD = df.select(
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
}

```

here the example makes use of the DataFrame API, but you can use the RDD API as well to read your data.
