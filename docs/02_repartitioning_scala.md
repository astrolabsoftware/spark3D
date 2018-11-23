---
permalink: /docs/repartitioning/scala/
layout: splash
title: "Tutorial: Partition your space"
date: 2018-06-15 22:31:13 +0200
---
# Tutorial: Partition your space

## Why partitioning the space?

Often the data is not written on disks the way we would like to have it distributed. Re-partitioning the space is a way to re-organise the data to speed-up later queries, exploration of the data set or visualisation.

Unfortunately, re-partitioning the space involves potentially large shuffle between executors which have to send and receive data according to the new partitioning scheme. Depending on the network capacity and the size of the initial data set, this operation can be costly and the re-partition is only a good idea if multiple queries have to be performed, or if you do not have other choices (like in visualisation).

You might noticed the DataFrame API does not expose many things to repartition your data as you would like (unlike the RDD API), hence we decided to bring here new DataFrame features!

## How spark3D partitions DataFrames?

The partitioning of a DataFrame is done in two steps.

- First we create a custom partitioner, that is an object which associates each object in the dataset with a partition number. Once this partitioner is known, we add a DataFrame column with the new partition ID (`addSPartitioning`).
- Second we trigger the partitioning based on this new column (`repartitionByCol`) using a simple `KeyPartitioner`.

Note that partitioning by a DataFrame column is similar to indexing a column in a relational database.

## Available partitioning and partitioners

There are currently 2 spatial partitioning implemented in the library:

- **Onion Partitioning:** See [here](https://github.com/astrolabsoftware/spark3D/issues/11) for a description. This is mostly intented for processing astrophysical data as it partitions the space in 3D shells along the radial axis, with the possibility of projecting into 2D shells.
- **Octree:** An octree extends a quadtree by using three orthogonal splitting planes to subdivide a tile into eight children. Like quadtrees, 3D Tiles allows variations to octrees such as non-uniform subdivision, tight bounding volumes, and overlapping children.

In addition, there is a simple `KeyPartitioner` available in case you bring your own partitioning scheme (see example below).

### An example: Octree partitioning

In the following example, we load 3D data, and we re-partition it with the octree partitioning (note that in practce you can have metadata in addition to the 3 coordinates):

```scala
// spark3D implicits
import com.astrolabsoftware.spark3d._

// Load data
val df = spark.read.format("fits")
	.option("hdu", 1)
	.load("src/test/resources/cartesian_points.fits")

df.show(5)
	+----------+-----------+----------+
	|         x|          y|         z|
	+----------+-----------+----------+
	| 0.5488135| 0.39217296|0.36925632|
	|0.71518934|0.041156586|  0.211326|
	|0.60276335| 0.92330056|0.47690478|
	| 0.5448832| 0.40623498|0.08223436|
	| 0.4236548|  0.9442822|0.23765936|
	+----------+-----------+----------+

// Specify options.
val options = Map(
	"geometry" -> "points",
	"colnames" -> "x,y,z",
	"coordSys" -> "cartesian",
	"gridtype" -> "octree")

// Add a column containing the future partition ID.
// Note that no shuffle has been done yet.
val df_colid = df.addSPartitioning(options, numPartitions=8)

df_colid.show(5)
	+----------+-----------+----------+------------+
	|         x|          y|         z|partition_id|
	+----------+-----------+----------+------------+
	| 0.5488135| 0.39217296|0.36925632|           7|
	|0.71518934|0.041156586|  0.211326|           7|
	|0.60276335| 0.92330056|0.47690478|           5|
	| 0.5448832| 0.40623498|0.08223436|           7|
	| 0.4236548|  0.9442822|0.23765936|           4|
	+----------+-----------+----------+------------+

// Trigger the repartition
val df_repart = df_colid.repartitionByCol("partition_id", preLabeled=true, numPartitions=8)

df_repart.show(5)
	+-----------+---------+----------+------------+
	|          x|        y|         z|partition_id|
	+-----------+---------+----------+------------+
	| 0.45615032|0.9558897| 0.9961785|           0|
	| 0.43703195|0.5811579|0.59632105|           0|
	| 0.16130951|0.8325458|0.72905064|           0|
	|  0.4686512|0.7746533| 0.8734174|           0|
	|0.064147495|0.5454706|0.83992904|           0|
	+-----------+---------+----------+------------+
```

`preLabeled` means the column containing the partition ID contains already numbers from 0 to `numPartitions - 1` (see below for the generic case).

Note that you can also use the DataFrame methods as standalone methods (not as DataFrame implicits):

```scala
import com.astrolabsoftware.spark3d.Repartitioning.addSPartitioning
import com.astrolabsoftware.spark3d.Repartitioning.repartitionByCol

val df_colid = addSPartitioning(df, options, numPartitions)
val df_repart = repartitionByCol(df_colid, "partition_id", preLabeled, numPartitions)
```

### Options for 3D repartitioning

The current options for spark3D repartitioning are:

- `gridtype`: the type of repartitioning. Available: current (no repartitioning), onion, octree.
- `geometry`: geometry of objects: points (3 coordinates), spheres (3 coordinates + radius)
- `coordSys`: coordinate system: spherical or cartesian
- `colnames`: comma-separated names of the spatial coordinates. For points,
    must be "x,y,z" or "r,theta,phi". For spheres, must be "x,y,z,R" or
    "r,theta,phi,R".

For example, to load point-like objects whose column coordinates are (x, y, z), in a cartesian coordinate system and repartition using octree, you would use:

```scala
val options = Map(
	"geometry" -> "points",
	"colnames" -> "x,y,z",
	"coordSys" -> "cartesian",
	"gridtype" -> "octree")
```

### Create your own Partitioning

spark3D contains limited number of partitioners. We plan to add more (RTree in preparation for example). In the meantime, you can add your partitioner, and use spark3D `repartitionByCol` to trigger the repartitioning.

Typically, a partitioner is an object which associate to each row a partition ID. So if you come up with your column containing partition ID, the job is done!

### Going beyond: column partitioning as a standard Spark SQL method

Actually you do not even need to know or create the underlined partitioners. Say you already have a column that described your partitioning, then you can directly apply `repartitionByCol`:

```scala
// spark3D implicits
import com.astrolabsoftware.spark3d._

df.show()
+---+----+
|Age|flag|
+---+----+
| 20|   0|
| 30|   1|
| 24|   0|
| 56|   0|
| 34|   1|
+---+----+

val df_repart = df.repartitionByCol("flag", preLabeled=true, numPartitions=2)

df_repart.show()
+---+----+
|Age|flag|
+---+----+
| 20|   0|
| 24|   0|
| 56|   0|
| 30|   1|
| 34|   1|
+---+----+

```

If your columns is not `preLabeled` (i.e. does not contain partition ID), this is not a problem:

```scala
// spark3D implicits
import com.astrolabsoftware.spark3d._

df.show()
+---+----+----+
|Age|flag|part|
+---+----+----+
| 20|   0|   A|
| 30|   2|   A|
| 24|   0|toto|
| 56|   0|   A|
| 34|   2|toto|
+---+----+----+

val df_repart = df.repartitionByCol("part", preLabeled=false)
df_repart.show()
+---+----+----+
|Age|flag|part|
+---+----+----+
| 20|   0|   A|
| 30|   2|   A|
| 56|   0|   A|
| 24|   0|toto|
| 34|   2|toto|
+---+----+----+

df_repart.rdd.getNumPartitions
2
```

Note however that the execution time will be longer since the routine needs to map column elements to distinct keys.

### How is spark3D `df.repartitionByCol` different from Apache Spark `df.repartitionByRange` or `df.orderBy`?

- `df.repartitionByRange` returns a `DataFrame` partitioned by the specified column(s). 
- `df.orderBy` returns a `DataFrame` partitioned and sorted by the specified column(s). 


A major problem of both is there is no guarantee to always get the same final partitioning. In other words, they are non-deterministic. See for example [SPARK-26024](https://issues.apache.org/jira/browse/SPARK-26024). For performance reasons they use sampling to estimate the ranges (with default size of 100). Hence, the output may not be consistent, since sampling can return different values.

Conversely `df.repartitionByCol` always guarantees the same final partitioning, by strictly following the partitioning scheme in the specified column. Obviously this exactness has a cost, and using `df.repartitionByCol` can increase the repartitioning time as shown in:

![raw]({{ "/assets/images/benchmark_repart_methods.png" | absolute_url }})

## Memory and speed considerations

We advice to cache the re-partitioned sets, to speed-up future call by not performing the re-partitioning again.
However keep in mind that if a large `numPartitions` decreases the cost of performing future queries (cross-match, KNN, ...), it increases the partitioning cost as more partitions implies more data shuffle between partitions. There is no magic number for `numPartitions ` which applies in general, and you'll need to set it according to the needs of your problem.

### What kind of performance I will get assuming fixed cluster configuration and input data set?

Assuming you use a fix cluster configuration (numbers of cores + available memory), the repartitioning time is quadratic in the final number of partitions:

![raw]({{ "/assets/images/complexity_repartitioning.png" | absolute_url }})

In fact, that also depends on the difference between the number of worker threads and the number of partitions:

![raw]({{ "/assets/images/complexity_repartitioning_explained.png" | absolute_url }})

### How can I check that my repartitioning is well balanced?

There is a method in spark3D to check the load balancing:

```scala
import com.astrolabsoftware.spark3d.Checkers.checkLoadBalancing

df_repart.checkLoadBalancing("frac").show()
+--------+------------+
|Load (%)|partition_id|
+--------+------------+
|   10.52|           0|
|   10.19|           1|
|   9.925|           2|
|    9.87|           3|
|  10.135|           4|
|   10.13|           5|
|    9.49|           6|
|    9.81|           7|
|    9.87|           8|
|   10.06|           9|
+--------+------------+
```