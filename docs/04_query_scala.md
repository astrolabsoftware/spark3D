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
import import com.astrolabsoftware.spark3d.Queries.KNN

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
val dfNeighbourWithMeta = df.join(dfNeighbour, Seq("x", "y", "z"), "left_semi")
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

To come

## Cross-match

To come
