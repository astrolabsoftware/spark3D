---
permalink: /docs/visualisation/scala/
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

- **Onion Partitioning:** See [here](https://github.com/astrolabsoftware/spark3D/issues/11) for a description. This is mostly intented for processing astrophysical data as it partitions the space in 3D shells along the radial axis, with the possibility of projecting into 2D shells (and then partitioning the shells using Healpix).
- **Octree:** An octree extends a quadtree by using three orthogonal splitting planes to subdivide a tile into eight children. Like quadtrees, 3D Tiles allows variations to octrees such as non-uniform subdivision, tight bounding volumes, and overlapping children.

### Onion Partitioning

In the following example, we load `Point3D` data, and we re-partition it with the onion partitioning

```scala
import com.astrolabsoftware.spark3d.spatial3DRDD.Point3DRDD
import com.astrolabsoftware.spark3d.utils.GridType

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

val spark = SparkSession.builder()
	.appName("OnionSpace")
	.getOrCreate()

// Data is in src/test/resources
val fn = "astro_obs.fits"
val columns = "Z_COSMO,RA,DEC"
val spherical = true
val format = "fits" // "com.astrolabsoftware.sparkfits"
val options = Map("hdu" -> "1")

// Load the data and cache the rawRDD to speed-up the re-partitioning
val pointRDD = new Point3DRDD(
	spark, fn, columns, spherical, format, options, StorageLevel.MEMORY_ONLY
)

// nPart is the wanted number of partitions. Default is pointRDD partition number.
val pointRDD_partitioned = pointRDD.spatialPartitioning(GridType.LINEARONIONGRID, nPart)
```

| Raw data set | Re-partitioned data set
|:---------:|:---------:
| ![raw]({{ "/assets/images/myOnionFigRaw.png" | absolute_url }}) | ![repartitioning]({{ "/assets/images/myOnionFig.png" | absolute_url }})

Color code indicates the partitions (all objects with the same color belong to the same partition).

### Octree Partitioning

In the following example, we load `ShellEnvelope` data (spheres), and we re-partition it with the octree partitioning

```scala
import com.astrolabsoftware.spark3d.spatial3DRDD.SphereRDD
import com.astrolabsoftware.spark3d.utils.GridType

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
	.appName("OctreeSpace")
	.getOrCreate()

// Data is in src/test/resources
val fn = "cartesian_spheres.fits"
val columns = "x,y,z,radius"
val spherical = false
val format = "fits" // com.astrolabsoftware.sparkfits
val options = Map("hdu" -> "1")

// Load the data and cache the rawRDD to speed-up the re-partitioning
val sphereRDD = new SphereRDD(
	spark, fn, columns, spherical, format, options, StorageLevel.MEMORY_ONLY
)

// nPart is the wanted number of partitions (floored to a power of 8).
// Default is sphereRDD partition number.
val pointRDD_partitioned = pointRDD.spatialPartitioning(GridType.OCTREE, nPart)
```


We advice to cache as well the re-partitioned sets, to speed-up future call by not performing the re-partitioning again. If you are short in memory, unpersist first the rawRDD before caching the repartitioned RDD.
However keep in mind that if a large `nPart` decreases the cost of performing future queries (cross-match, KNN, ...), it increases the partitioning cost as more partitions implies more data shuffle between partitions. There is no magic number for `nPart` which applies in general, and you'll need to set it according to the needs of your problem. My only advice would be: re-partitioning is typically done once, queries can be multiple...

| Raw data set | Re-partitioned data set
|:---------:|:---------:
| ![raw]({{ "/assets/images/rawData_noOctree.png" | absolute_url }}) | ![repartitioning]({{ "/assets/images/rawData_withOctree.png" | absolute_url }})

Color code indicates the partitions (all objects with the same color belong to the same partition).

## Current benchmark

TBD
