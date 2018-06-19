---
permalink: /docs/partitioning/
layout: splash
title: "Tutorial: Partition your space"
date: 2018-06-18 22:31:13 +0200
---

# Tutorial: Partition your space

## Why partitioning the space?

Often the data is not written on disks the way we would like to have it distributed. Re-partitioning the space is a way to re-organise the data to speed-up later queries and explorations of the data set.

Unfortunately, re-partitioning the space involves potentially large shuffle between executors which have to send and receive data according to the new wanted scheme. Depending on the network capacity and the size of the initial data set, this operation can be costly and the re-partition is only a good idea if multiple queries have to be performed.

## Available partitioning and partitioners

There are currently 2 partitioning implemented in the library:

- **Onion Partitioning:** See [here](https://github.com/JulienPeloton/spark3D/issues/11) for a description.
- **Octree:** An octree extends a quadtree by using three orthogonal splitting planes to subdivide a tile into eight children. Like quadtrees, 3D Tiles allows variations to octrees such as non-uniform subdivision, tight bounding volumes, and overlapping children.

In the following example, we load `Point3D` data, and we re-partition it with the onion partitioning

```scala
import com.spark3d.spatial3DRDD._
import com.spark3d.utils.GridType
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
	.appName("OnionSpace")
	.getOrCreate()

// Data is in src/test/resources
val fn = "astro_obs.fits"
val hdu = 1
val columns = "Z_COSMO,RA,DEC"
val spherical = true

// Load the data
val pointRDD = new Point3DRDDFromFITS(spark, fn, hdu, columns, spherical)

// As this example is done in local mode, and the file is very small,
// the RDD pointRDD has only 1 partition. For the sake of this example,
// let's increase the number of partition to 5. If no number of partition is
// given, the new partitioning has the same number of partition as the old ones.
val pointRDD_part = pointRDD.spatialPartitioning(GridType.LINEARONIONGRID, 5)
```

| Raw data set | Re-partitioned data set |
|:---------:|:---------:|
| ![raw]({{ "/assets/images/myOnionFigRaw.jpg" | absolute_url }}) | ![repartitioning]({{ "/assets/images/myOnionFig.jpg" | absolute_url }})|

## Current benchmark

TBD
