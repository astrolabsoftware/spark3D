---
permalink: /docs/visualisation/python/
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

```python
# Launch this example: spark-submit --master ... --packages spark3D_id
from pyspark.sql import SparkSession
from pyspark3d.spatial3DRDD import Point3DRDD

# Initialise Spark Session
spark = SparkSession.builder\
    .appName("collapse")\
    .getOrCreate()

# Load the data
fn = "src/test/resources/astro_obs.fits"
p3drdd = Point3DRDD(spark, fn, "Z_COSMO,RA,DEC", True, "fits", {"hdu": "1"})

# nPart is the wanted number of partitions.
# Default is rdd.rawRDD() partition number.
npart = 5
gridtype = "LINEARONIONGRID"
rdd_part = p3drdd.spatialPartitioningPython(gridtype, npart)
```

| Raw data set | Re-partitioned data set
|:---------:|:---------:
| ![raw]({{ "/assets/images/onion_nopart_python.png" | absolute_url }}) | ![repartitioning]({{ "/assets/images/onion_part_python.png" | absolute_url }})

Color code indicates the partitions (all objects with the same color belong to the same partition).

### Octree Partitioning

In the following example, we load `ShellEnvelope` data (spheres), and we re-partition it with the octree partitioning

```python
# Launch this example: spark-submit --master ... --packages spark3D_id
from pyspark.sql import SparkSession
from pyspark3d.spatial3DRDD import SphereRDD

# Initialise Spark Session
spark = SparkSession.builder\
    .appName("collapse")\
    .getOrCreate()

# Load the data
fn = "src/test/resources/cartesian_spheres.fits"
srdd = SphereRDD(spark, fn, "x,y,z,radius", False, "fits", {"hdu": "1"})

# nPart is the wanted number of partitions (floored to a power of 8).
# Default is rdd.rawRDD() partition number.
npart = 10
gridtype = "OCTREE"
rdd_part = srdd.spatialPartitioningPython(gridtype, npart)
```


We advice to cache as well the re-partitioned sets, to speed-up future call by not performing the re-partitioning again. If you are short in memory, unpersist first the rawRDD before caching the repartitioned RDD.
However keep in mind that if a large `nPart` decreases the cost of performing future queries (cross-match, KNN, ...), it increases the partitioning cost as more partitions implies more data shuffle between partitions. There is no magic number for `nPart` which applies in general, and you'll need to set it according to the needs of your problem. My only advice would be: re-partitioning is typically done once, queries can be multiple...

| Raw data set | Re-partitioned data set
|:---------:|:---------:
| ![raw]({{ "/assets/images/octree_nopart_python.png" | absolute_url }}) | ![repartitioning]({{ "/assets/images/octree_part_python.png" | absolute_url }})

Size of the markers is proportional to the radius size. Color code indicates the partitions (all objects with the same color belong to the same partition).

## Current benchmark

TBD
