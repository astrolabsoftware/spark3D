---
layout: post
title: "Tutorial: Introduction"
date: 2018-06-15
---

# Tutorial: Introduction

## Loading data with spark3D

In this tutorial we will review the steps to simply create RDD from 3D data sets. A 3DRDD is simply a RDD whose elements are 3D objects. Currently, spark3D supports 3 kind of objects: points (`Point3D`), spherical shells (`ShellEnvelope`), and boxes (`BoxEnvelope`). Note that spheres are a subcase of shells.

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

Feel free to submit a request for having new data source!
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

here the example make uses of DataFrame, but you can use RDD method as well to read your data.
