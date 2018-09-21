---
permalink: /docs/query/python/
layout: splash
title: "Tutorial: Query, cross-match, play!"
date: 2018-06-18 22:31:13 +0200
---

# Tutorial: Query, cross-match, play!

The spark3D library contains a number of methods and tools to manipulate 3D RDD. Currently, you can already play with *window query*, *KNN* and *cross-match between data sets*.

## Envelope query

A Envelope query takes as input a `RDD[Shape3D]` and an envelope, and returns all objects in the RDD intersecting the envelope (contained in and crossing the envelope):

```python
# Launch this example: spark-submit --master ... --packages spark3D_id
from pyspark.sql import SparkSession
from pyspark3d.geometryObjects import ShellEnvelope
from pyspark3d.spatial3DRDD import SphereRDD
from pyspark3d.spatialOperator import windowQuery

# Initialise Spark Session
spark = SparkSession.builder\
    .appName("collapse")\
    .getOrCreate()

# Load the data
fn = "src/test/resources/cartesian_spheres.fits"
rdd = SphereRDD(spark, fn, "x,y,z,radius", False, "fits", {"hdu": "1"})

# Load the envelope (Sphere at the origin, and radius 0.5)
sh = ShellEnvelope(0.0, 0.0, 0.0, False, 0.0, 0.5)

# Perform the query
matchRDD = windowQuery(rdd.rawRDD(), sh)
print("{}/{} objects found in the envelope".format(
  len(matchRDD.collect()), rdd.rawRDD().count()))
# 1435/20000 objects found in the envelope
```

Note that the input objects and the envelope can be anything among the `Shape3D`: points, shells (incl. sphere), boxes.

## Cross-match between data-sets

A cross-match takes as input two data sets, and return objects matching based on the center distance, or pixel index of objects. Note that performing a cross-match between a data set of N elements and another of M elements is a priori a NxM operation - so it can be very costly! Let's load two `Point3D` data sets:

```python
# Launch this example: spark-submit --master ... --packages spark3D_id
from pyspark.sql import SparkSession
from pyspark3d.spatial3DRDD import Point3DRDD

# Initialise Spark Session
spark = SparkSession.builder\
    .appName("collapse")\
    .getOrCreate()

# Load the raw data (spherical coordinates)
fn = "src/test/resources/astro_obs_{}_light.fits"
rddA = Point3DRDD(
  spark, fn.format("A"), "Z_COSMO,RA,DEC", True, "fits", {"hdu": "1"})
rddB = Point3DRDD(
  spark, fn.format("B"), "Z_COSMO,RA,DEC", True, "fits", {"hdu": "1"})
```

By default, the two sets are partitioned randomly (in the sense points spatially close are probably not in the same partition).
In order to decrease the cost of performing the cross-match, you need to partition the two data sets the same way. By doing so, you will cross-match only points belonging to the same partition. For a large number of partitions, you will decrease significantly the cost:

```python
# nPart is the wanted number of partitions.
# Default is rdd.rawRDD() partition number.
npart = 100

# For the spatial partitioning, you can currently choose
# between LINEARONIONGRID, or OCTREE (see GridType.scala).
rddA_part = rddA.spatialPartitioningPython("LINEARONIONGRID", 100)

# Repartition B as A
rddB_part = rddB.spatialPartitioningPython(
  rddA_part.partitioner().get()).cache()
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

Here is an example which returns only elements from A with counterpart in B using distance center:

```python
from pyspark3d.spatialOperator import CrossMatchCenter

# Distance threshold for the match
epsilon = 0.04

# Keeping only elements from A with counterpart in B
matchRDDB = CrossMatchCenter(rddA_part, rddB_part, epsilon, "A")

print("{}/{} elements in A match with elements of B!".format(
  matchRDDB.count(), rddB_part.count()))
# 19/1000 elements in A match with elements of B!
```

and the same using the Healpix indices:

```python
from pyspark3d.spatialOperator import PixelCrossMatch

# Shell resolution for Healpix indexing
nside = 512

# Keeping only elements from A with counterpart in B
matchRDDB_healpix = CrossMatchHealpixIndex(rddA_part, rddB_part, 512, "A")
print("{}/{} elements in A match with elements of B!".format(
  matchRDDB_healpix.count(), rddB_part.count()))
# 15/1000 elements in A match with elements of B!
```

In addition, you can choose to return only the Healpix indices for which points match (returnType="healpix"). It is even faster than returning objects.

## Neighbour search

### Simple KNN

Finds the K nearest neighbours of a query object within a `rdd`.
The naive implementation here searches through all the the objects in the
RDD to get the KNN. The nearness of the objects here is decided on the
basis of the distance between their centers.
Note that `queryObject` and elements of `rdd` must have the same type
(either both Point3D, or both ShellEnvelope, or both BoxEnvelope).

```python
# Launch this example: spark-submit --master ... --packages spark3D_id
from pyspark.sql import SparkSession
from pyspark3d.geometryObjects import Point3D
from pyspark3d.spatial3DRDD import Point3DRDD
from pyspark3d.spatialOperator import KNN

# Initialise Spark Session
spark = SparkSession.builder\
    .appName("collapse")\
    .getOrCreate()

# Load the data (spherical coordinates)
fn = "src/test/resources/astro_obs.fits"
rdd = Point3DRDD(spark, fn, "Z_COSMO,RA,DEC", True, "fits", {"hdu": "1"}
)

# Define your query point
pt = Point3D(0.0, 0.0, 0.0, True)

# Perform the query: look for the 5 closest neighbours from `pt`.
# Note that the last argument controls whether we want to eliminate duplicates.
match = KNN(rdd.rawRDD(), pt, 5, True)

# `match` is a list of Point3D. Take the coordinates and convert them
# into cartesian coordinates for later use:
mod = "com.astrolabsoftware.spark3d.utils.Utils.sphericalToCartesian"
converter = load_from_jvm(mod)
match_coord = [converter(m).getCoordinatePython() for m in match]

# Print the distance to the query point (origin)
def normit(l: list) -> float:
  """
  Return the distance to the origin for each point of the list

  Parameters
  ----------
  l : list of list
    List of (list of 3 float). Each main list element represents
    the cartesian coordinates of a point in space.

  Returns
  ----------
  ld : list of float
    list containing (euclidean) norm btw each point and the origin.
  """
  return sqrt(sum([e**2 for e in l]))

# Pretty print
print([str(normit(l) * 1e6) + "e-6" for l in match_coord])
# [72, 73, 150, 166, 206]
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

```python
# Launch this example: spark-submit --master ... --packages spark3D_id
from pyspark.sql import SparkSession
from pyspark3d.geometryObjects import Point3D
from pyspark3d.spatial3DRDD import Point3DRDD
from pyspark3d.spatialOperator import KNN

# Initialise Spark Session
spark = SparkSession.builder\
    .appName("collapse")\
    .getOrCreate()

# Load the data (spherical coordinates)
fn = "src/test/resources/astro_obs.fits"
rdd = Point3DRDD(spark, fn, "Z_COSMO,RA,DEC", True, "fits", {"hdu": "1"}
)

# Repartition the RDD
rdd_part = rdd.spatialPartitioningPython("LINEARONIONGRID", 5)

# Define your query point
pt = Point3D(0.0, 0.0, 0.0, True)

# Perform the query: look for the 5 closest neighbours from `pt`.
# Automatically discards duplicates
match = KNNEfficient(rdd_part, pt, 5)

# `match` is a list of Point3D. Take the coordinates and convert them
# into cartesian coordinates for later use:
mod = "com.astrolabsoftware.spark3d.utils.Utils.sphericalToCartesian"
converter = load_from_jvm(mod)
match_coord = [converter(m).getCoordinatePython() for m in match]

# Print the distance to the query point (origin)
def normit(l: list) -> float:
  """
  Return the distance to the origin for each point of the list

  Parameters
  ----------
  l : list of list
    List of (list of 3 float). Each main list element represents
    the cartesian coordinates of a point in space.

  Returns
  ----------
  ld : list of float
    list containing (euclidean) norm btw each point and the origin.
  """
  return sqrt(sum([e**2 for e in l]))

# Pretty print
print([str(normit(l) * 1e6) + "e-6" for l in match_coord])
# [72, 73, 150, 166, 206]
```

## Benchmarks

TBD
