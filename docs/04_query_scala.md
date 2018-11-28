---
permalink: /docs/query/scala/
layout: splash
title: "Tutorial: Query, cross-match, play!"
date: 2018-06-18 22:31:13 +0200
---

# Tutorial: Query, cross-match, play!

The spark3D library contains a number of methods and tools to manipulate 3D RDD. Currently, you can already play with *window query*, *KNN* and *cross-match between data sets*.

## Neighbour search

### Standard KNN

Finds the K nearest neighbours of an object within a `DataFrame`.
The naive implementation here searches through all the the objects in the
DataFrame to get the KNN. The nearness of the objects here is decided on the
basis of the distance between their centers.

```scala
import com.astrolabsoftware.spark3d.Queries.KNN

// Let's suppose we have a DataFrame containing at least
// 3 columns x, y, z corresponding to cartesian coordinate of points.
val df = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .load("src/test/resources/cartesian_spheres_manual.csv")

df.show()
  +---+---+---+------+
  |  x|  y|  z|radius|
  +---+---+---+------+
  |1.0|1.0|1.0|   0.8|
  |1.0|1.0|1.0|   0.5|
  |3.0|1.0|1.0|   0.8|
  |3.0|1.0|1.0|   0.5|
  |3.0|3.0|1.0|   0.8|
  |3.0|3.0|1.0|   0.5|
  |1.0|3.0|1.0|   0.8|
  |1.0|3.0|1.0|   0.5|
  |1.0|1.0|3.0|   0.8|
  |1.0|1.0|3.0|   0.5|
  |3.0|1.0|3.0|   0.8|
  |3.0|1.0|3.0|   0.5|
  |3.0|3.0|3.0|   0.8|
  |3.0|3.0|3.0|   0.5|
  |1.0|3.0|3.0|   0.8|
  |2.0|2.0|2.0|   2.0|
  +---+---+---+------+

// Define your target for which you want the neighbour points
val coordSys : String = "cartesian"
val target : List[Double] = List(0.2, 0.2, 0.2)

// Number of neighbours
val K : Int = 2

// Return a DataFrame
val dfNeighbour = KNN(df.select("x", "y", "z"), target, K, coordSys, unique = false)
dfNeighbour.show()
  +---+---+---+
  |  x|  y|  z|
  +---+---+---+
  |1.0|1.0|1.0|
  |1.0|1.0|1.0|
  +---+---+---+

// In case you want also the metadata, you can perform a join with initial DataFrame:
val dfNeighbourWithMeta = df.join(dfNeighbour, dfNeighbour.columns, "left_semi")
dfNeighbourWithMeta.show()
  +---+---+---+------+
  |  x|  y|  z|radius|
  +---+---+---+------+
  |1.0|1.0|1.0|   0.8|
  |1.0|1.0|1.0|   0.5|
  +---+---+---+------+
```

Note that by default (`unique = false`), the method `KNN` will return exactly `K` neighbours
regardless their values. But there could be neighbours at the same location. If you want exactly `K` distinct neighbours, then choose `unique = true`.

### Accelerated KNN

To come

## Window Query

A window query takes as input a `DataFrame` and an envelope, and returns all objects in the RDD intersecting the envelope (contained in and crossing the envelope):

```scala
import com.astrolabsoftware.spark3d.Queries.windowQuery

// Let's suppose we have a DataFrame containing at least
// 3 columns x, y, z corresponding to cartesian coordinate of points.
val df = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .load("src/test/resources/cartesian_spheres_manual.csv")

df.show()
  +---+---+---+------+
  |  x|  y|  z|radius|
  +---+---+---+------+
  |1.0|1.0|1.0|   0.8|
  |1.0|1.0|1.0|   0.5|
  |3.0|1.0|1.0|   0.8|
  |3.0|1.0|1.0|   0.5|
  |3.0|3.0|1.0|   0.8|
  |3.0|3.0|1.0|   0.5|
  |1.0|3.0|1.0|   0.8|
  |1.0|3.0|1.0|   0.5|
  |1.0|1.0|3.0|   0.8|
  |1.0|1.0|3.0|   0.5|
  |3.0|1.0|3.0|   0.8|
  |3.0|1.0|3.0|   0.5|
  |3.0|3.0|3.0|   0.8|
  |3.0|3.0|3.0|   0.5|
  |1.0|3.0|3.0|   0.8|
  |2.0|2.0|2.0|   2.0|
  +---+---+---+------+

// Define your envelope - here we take a sphere
// centred on (2.0, 1.0, 2.0) with radius 1.8
val windowtype = "sphere"
val windowcoord = List(2.0, 1.0, 2.0, 1.8)
val coordSys = "cartesian"

// Return a DataFrame
val env = windowQuery(df.select("x", "y", "z"), windowtype, windowcoord, coordSys)
env.show()
  +---+---+---+
  |  x|  y|  z|
  +---+---+---+
  |1.0|1.0|1.0|
  |1.0|1.0|1.0|
  |3.0|1.0|1.0|
  |3.0|1.0|1.0|
  |1.0|1.0|3.0|
  |1.0|1.0|3.0|
  |3.0|1.0|3.0|
  |3.0|1.0|3.0|
  |2.0|2.0|2.0|
  +---+---+---+
// In case you want also the metadata, you can perform a join with initial DataFrame:
envWithMeta = df.join(env, env.columns, "left_semi")
envWithMeta.show()
  +---+---+---+------+
  |  x|  y|  z|radius|
  +---+---+---+------+
  |3.0|1.0|1.0|   0.8|
  |3.0|1.0|1.0|   0.5|
  |3.0|1.0|3.0|   0.8|
  |3.0|1.0|3.0|   0.5|
  |2.0|2.0|2.0|   2.0|
  |1.0|1.0|1.0|   0.8|
  |1.0|1.0|1.0|   0.5|
  |1.0|1.0|3.0|   0.8|
  |1.0|1.0|3.0|   0.5|
  +---+---+---+------+
```

Note that the envelope can be anything among the `Shape3D`: point, shell, sphere, box.
Depending on the `windowtype`, you would express `windowcoord` as:
- point: `windowcoord` = `List(x, y, z)`
- sphere: `windowcoord` = `List(x, y, z, R)`
- shell: `windowcoord` = `List(x, y, z, Rin, Rout)`
- box: `windowcoord` = `List(x1, y1, z1, x2, y2, z2, x3, y3, z3)`

Use `(x, y, z)` for cartesian or `(r, theta, phi)` for spherical.
Note that box only accepts cartesian coordinates.

## Cross-match

To come
