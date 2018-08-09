---
permalink: /docs/query/scala/
layout: splash
title: "Tutorial: Query, cross-match, play!"
date: 2018-06-18 22:31:13 +0200
---

# Tutorial: Query, cross-match, play!

The spark3D library contains a number of methods and tools to manipulate 3D RDD. Currently, you can already play with *window query*, *KNN* and *cross-match between data sets*.

## Envelope query

A Envelope query takes as input a `RDD[Shape3D]` and an envelope, and returns all objects in the RDD intersecting the envelope (contained in and crossing the envelope):

```scala
import com.astrolabsoftware.spark3d.spatial3DRDD.Point3DRDD
import com.astrolabsoftware.spark3d.geometryObjects.{Point3D, ShellEnvelope}
import com.astrolabsoftware.spark3d.spatialOperator.RangeQuery

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
    .appName("Query")
    .getOrCreate()

// Data files are in src/test/resources
val fn = "astro_obs.fits"
val columns = "Z_COSMO,RA,DEC"
val spherical = true
val format = "fits" // "com.astrolabsoftware.sparkfits"
val options = Map("hdu" -> "1")

// Load a RDD[Point3D] from the FITS file
val objects = new Point3DRDD(spark, fn, columns, spherical, format, options)

// Define our envelope, here a sphere.
val center = new Point3D(0.9, 0.0, 0.0, spherical)
val radius = 0.1
val envelope = new ShellEnvelope(center, radius)

// Perform the match
val queryResult = RangeQuery.windowQuery(objects, envelope)
```

Note that the input objects and the envelope can be anything among the `Shape3D`: points, shells (incl. sphere), boxes.

Envelope = Sphere |Envelope = Box
:-------------------------:|:-------------------------:
![raw]({{ "/assets/images/sphereMatch.png" | absolute_url }})| ![raw]({{ "/assets/images/BoxMatch.png" | absolute_url }})

## Cross-match between data-sets

A cross-match takes as input two data sets, and return objects matching based on the center distance, or pixel index of objects. Note that performing a cross-match between a data set of N elements and another of M elements is a priori a NxM operation - so it can be very costly! Let's load two `Point3D` data sets:

```scala
import com.astrolabsoftware.spark3d.spatial3DRDD.Point3DRDD

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
	.appName("Xmatch")
	.getOrCreate()

// Data files are in src/test/resources
val fnA = "astro_obs.fits"
val fnB = "astro_obs2.fits"
val columns = "Z_COSMO,RA,DEC"
val spherical = true
val format = "fits" // "com.astrolabsoftware.sparkfits"
val options = Map("hdu" -> "1")

// Load the two data sets
val setA = new Point3DRDD(spark, fnA, hdu, columns, spherical, format, options)
val setB = new Point3DRDD(spark, fnB, hdu, columns, spherical, format, options)
```

By default, the two sets are partitioned randomly (in the sense points spatially close are probably not in the same partition).
In order to decrease the cost of performing the cross-match, you need to partition the two data sets the same way. By doing so, you will cross-match only points belonging to the same partition. For a large number of partitions, you will decrease significantly the cost:

```scala
import com.astrolabsoftware.spark3d.utils.GridType
import com.astrolabsoftware.spark3d.spatialPartitioning.SpatialPartitioner

// nPart is the wanted number of partitions. Default is setA_raw partition number.
// For the spatial partitioning, you can currently choose between LINEARONIONGRID, or OCTREE.
val setA_partitioned = setA_raw.spatialPartitioning(GridType.LINEARONIONGRID, nPart).cache()
// Get the partitioner of A
val partitionerA = setA_partitioned.partitioner.get.asInstanceOf[SpatialPartitioner]
// Repartition B as A
val setB_partitioned = setB_raw.spatialPartitioning(partitionerA).cache()
```

We advice to cache the re-partitioned sets, to speed-up future call by not performing the re-partitioning again.
However keep in mind that if a large `nPart` decreases the cost of performing the cross-match, it increases the partitioning cost as more partitions implies more data shuffle between partitions. There is no magic number for `nPart` which applies in general, and you'll need to set it according to the needs of your problem. My only advice would be: re-partitioning is typically done once, queries can be multiple...

### What a cross-match returns?

In spark3D, the cross-match between two sets A and B can return:

* (1) Elements of (A, B) matching (returnType="AB")
* (2) Elements of A matching B (returnType="A")
* (3) Elements of B matching A (returnType="B")

Which one you should choose? That depends on what you need:
(1) gives you all pairs matching but can be slow.
(2) & (3) give you all elements matching only in one side but is faster.

### What is the criterion for the cross-match?

Currently, we implemented two methods to perform a cross-match:

* Based on center distance (a and b match if norm(a - b) < epsilon).
* Based on the center angular separation (Healpix index) inside a shell (a and b match if their healpix index is the same). Note that this strategy can be used only in combination with the `LINEARONIONGRID` partitioning which produces 3D shells along the radial axis, and project the data in 2D shells (where Healpix can be used!).

Here is an example which returns only elements from B with counterpart in A using distance center:

```scala
import com.astrolabsoftware.spark3d.spatialOperator.CenterCrossMatch

// Distance threshold for the match
val epsilon = 0.004

// Keeping only elements from B with counterpart in A
val xMatchCenter = CenterCrossMatch
	.CrossMatchCenter(pointRDD_partA, pointRDD_partB, epsilon, "B")
```

and the same using the Healpix indices:

```scala
import com.astrolabsoftware.spark3d.spatialOperator.PixelCrossMatch

// Shell resolution for Healpix indexing
val nside = 512

// Keeping only elements from B with counterpart in A
val xMatchHealpix = PixelCrossMatch
	.CrossMatchHealpixIndex(setA_partitioned, setB_partitioned, nside, "B")
```

In addition, you can choose to return only the Healpix indices for which points match (returnType="healpix"). It is even faster than returning objects.

Here is a plot showing match inside one partition:

Cross match based on angular separation (A, B, AxB)    |Cross match based on angular separation (AxB, BxA)   
:-------------------------:|:-------------------------:
![raw]({{ "/assets/images/crossmatchAxB.png" | absolute_url }})| ![raw]({{ "/assets/images/crossmatchAxBOnly.png" | absolute_url }})

Cross match based on center distance (A, B, BxA)    |Cross match based on center distance (AxB, BxA)   
:-------------------------:|:-------------------------:
![raw]({{ "/assets/images/CcrossmatchAxB.png" | absolute_url }}) | ![raw]({{ "/assets/images/CcrossmatchAxBOnly.png" | absolute_url }})

For more details on the cross-match, see the following [notebook](https://github.com/astrolabsoftware/spark3D/blob/master/examples/jupyter/CrossMatch.ipynb).

## Neighbour search


### Simple KNN

Finds the K nearest neighbours of a query object within a `rdd`.
The naive implementation here searches through all the the objects in the
RDD to get the KNN. The nearness of the objects here is decided on the
basis of the distance between their centers.
Note that `queryObject` and elements of `rdd` must have the same type
(either both Point3D, or both ShellEnvelope, or both BoxEnvelope).

```scala
// Load the data
val pRDD = new Point3DRDD(spark, fn, columns, isSpherical, "csv", options)

// Centre object for the query
val queryObject = new Point3D(0.0, 0.0, 0.0, false)

// Find the `nNeighbours` closest neighbours
// Note that the last argument controls whether we want to eliminate duplicates.
val knn = SpatialQuery.KNN(pRDD.rawRDD, queryObject, nNeighbours, unique)
```

### More efficient KNN

More efficient implementation of the KNN query above.
First we seek the partitions in which the query object belongs and we
will look for the knn only in those partitions. After this if the limit k
is not satisfied, we keep looking similarly in the neighbors of the
containing partitions.

Note 1: elements of `rdd` and `queryObject` can have different types
among Shape3D (Point3D or ShellEnvelope or BoxEnvelope) (unlike KNN above).

Note 2: KNNEfficient only works on repartitioned RDD (python version).

```scala
// Load the data
val pRDD = new Point3DRDD(spark, fn, columns, isSpherical, "csv", options)

// Repartition the data
pRDD_part = pRDD.spatialPartitioning(GridType.LINEARONIONGRID, 100)

// Centre object for the query
val queryObject = new Point3D(0.0, 0.0, 0.0, false)

// Find the `nNeighbours` closest neighbours
// Automatically discards duplicates
val knn = SpatialQuery.KNNEfficient(pRDD_part, queryObject, nNeighbours)
```

## Benchmarks

TBD
