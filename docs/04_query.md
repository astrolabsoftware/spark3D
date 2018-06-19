---
permalink: /docs/query/
layout: splash
title: "Tutorial: Query, cross-match, play!"
date: 2018-06-18 22:31:13 +0200
---

# Tutorial: Query, cross-match, play!

The spark3D library contains a number of methods and tools to manipulate 3D RDD. Currently, you can already play with *neighbours search* and *data set cross-match*.

For more details on the cross-match, see the following [notebook](https://github.com/JulienPeloton/spark3D/blob/master/examples/jupyter/CrossMatch.ipynb).

## Cross-match between data-sets

Load the two `Point3D` data sets:

```scala
import com.spark3d.spatial3DRDD._
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder()
	.appName("Xmatch")
	.getOrCreate()

// Data files are in src/test/resources
val fnA = "astro_obs.fits"
val fnB = "astro_obs2.fits"
val hdu = 1
val columns = "Z_COSMO,RA,DEC"
val spherical = true

// Load the two data sets
val pointRDDA = new Point3DRDDFromFITS(spark, fnA, hdu, columns, spherical)
val pointRDDB = new Point3DRDDFromFITS(spark, fnB, hdu, columns, spherical)
```

By default, the pointRDD are partitioned randomly (in the sense points spatially close are likely not in the same partition). Let's repartition our data based on the distance to the center (Onion Space). In addition, to ease the cross-match between the two data sets, let's partition A and B the same way:

```scala
import com.spark3d.utils.GridType
import com.spark3d.spatialPartitioning.SpatialPartitioner

// As we are in local mode, and the file is very small,
// the RDD pointRDD has only 1 partition.
// For the sake of this example, let's increase the number of partition to 100.
val pointRDD_partA = pointRDDA.spatialPartitioning(GridType.LINEARONIONGRID, 100).cache()
// Get the partitioner of A
val partitionerA = pointRDD_partA.partitioner.get.asInstanceOf[SpatialPartitioner]
// Repartition B as A
val pointRDD_partB = pointRDDB.spatialPartitioning(partitionerA).cache()
```

### Cross matching using Healpix index

Cross match 2 RDD based on the healpix index of geometry center.
You have to choice to return:

* (1) Elements of (A, B) matching (returnType="AB")
* (2) Elements of A matching B (returnType="A")
* (3) Elements of B matching A (returnType="B")
* (4) Healpix pixel indices matching (returnType="healpix")

Which one you should choose? That depends on what you need:
(1) gives you all elements but is slow.
(2) & (3) give you all elements only in one side but is faster.
(4) gives you only healpix center but is even faster.

```scala
import com.spark3d.spatialOperator.PixelCrossMatch

// Shell resolution
val nside = 512
// Keeping only elements from A with counterpart in B
val xMatchA = PixelCrossMatch
	.CrossMatchHealpixIndex(pointRDD_partA, pointRDD_partB, nside, "A")
// Keeping only elements from B with counterpart in A
val xMatchB = PixelCrossMatch
	.CrossMatchHealpixIndex(pointRDD_partA, pointRDD_partB, nside, "B")
// Keeping all elements with counterparts in both A and B
val xMatchAB = PixelCrossMatch
	.CrossMatchHealpixIndex(pointRDD_partA, pointRDD_partB, nside, "AB")

println("Keeping only elements from A with counterpart in B: ", xMatchA.count(), " points")
// (Keeping only elements from A with counterpart in B: ,1337, points)             
println("Keeping only elements from B with counterpart in A: ", xMatchB.count(), " points")
// (Keeping only elements from B with counterpart in A: ,1278, points)
println("Keeping all elements with counterparts in both A and B: ", xMatchAB.count(), " points")
// (Keeping all elements with counterparts in both A and B: ,1382, points)
```

### Cross matching using distance between object centers

Cross match 2 RDD based on based on the object centers.
You have to choice to return:

* (1) Elements of (A, B) matching (returnType="AB")
* (2) Elements of A matching B (returnType="A")
* (3) Elements of B matching A (returnType="B")

Which one you should choose? That depends on what you need:
(1) gives you all elements but is slow.
(2) & (3) give you all elements only in one side but is faster.

```scala
import com.spark3d.spatialOperator.CenterCrossMatch

// Distance threshold for the match
val epsilon = 0.004
// Keeping only elements from A with counterpart in B
val CxMatchA = CenterCrossMatch
	.CrossMatchCenter(pointRDD_partA, pointRDD_partB, epsilon, "A")
// Keeping only elements from B with counterpart in A
val CxMatchB = CenterCrossMatch
	.CrossMatchCenter(pointRDD_partA, pointRDD_partB, epsilon, "B")
// Keeping all elements with counterparts in both A and B
val CxMatchAB = CenterCrossMatch
	.CrossMatchCenter(pointRDD_partA, pointRDD_partB, epsilon, "AB")

println("Keeping only elements from A with counterpart in B: ", CxMatchA.count(), " points")
// (Keeping only elements from A with counterpart in B: ,695, points)
println("Keeping only elements from B with counterpart in A: ", CxMatchB.count(), " points")
// (Keeping only elements from B with counterpart in A: ,497, points)
println("Keeping all elements with counterparts in both A and B: ", CxMatchAB.count(), " points")
// (Keeping all elements with counterparts in both A and B: ,6121, points)
```

Here is a plot for Partition #5

Cross match based on angular separation (A, B, AxB)    |Cross match based on angular separation (AxB, BxA)   
:-------------------------:|:-------------------------:
![raw]({{ "/assets/images/crossmatchAxB.png" | absolute_url }})| ![raw]({{ "/assets/images/crossmatchAxBOnly.png" | absolute_url }})
Cross match based on center distance (A, B, BxA)    |Cross match based on center distance (AxB, BxA)   
:-------------------------:|:-------------------------:
![raw]({{ "/assets/images/CcrossmatchAxB.png" | absolute_url }}) | ![raw]({{ "/assets/images/CcrossmatchAxBOnly.png" | absolute_url }})

## Neighbour search

TBD

## Benchmarks

TBD
