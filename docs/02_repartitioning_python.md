---
permalink: /docs/repartitioning/python/
layout: splash
title: "Tutorial: Partition your space"
date: 2018-06-15 22:31:13 +0200
---

# Tutorial: Partition your space

## Why partitioning the space?

Often the data is not written on disks the way we would like to have it distributed. Re-partitioning the space is a way to re-organise the data to speed-up later queries, exploration of the data set or visualisation.

Unfortunately, re-partitioning the space involves potentially large shuffle between executors which have to send and receive data according to the new partitioning scheme. Depending on the network capacity and the size of the initial data set, this operation can be costly and the re-partition is only a good idea if multiple queries have to be performed, or if you do not have other choices (like in visualisation).

You might noticed the DataFrame API does not expose many things to repartition your data as you would like (unlike the RDD API), hence we decided to bring here new DataFrame features!

## How spark3D partitions things?

The partitioning of a DataFrame is done in two steps.

- First we create a custom partitioner, that is an object which associates each object in the dataset with a partition number. Once this partitioner is known, we add a DataFrame column with the new partition ID (`addSPartitioning`).
- Second we trigger the partitioning based on this new column (`repartitionByCol`) using a simple `KeyPartitioner`.

## Available partitioning and partitioners

There are currently 2 spatial partitioning implemented in the library:

- **Onion Partitioning:** See [here](https://github.com/astrolabsoftware/spark3D/issues/11) for a description. This is mostly intented for processing astrophysical data as it partitions the space in 3D shells along the radial axis, with the possibility of projecting into 2D shells.
- **Octree:** An octree extends a quadtree by using three orthogonal splitting planes to subdivide a tile into eight children. Like quadtrees, 3D Tiles allows variations to octrees such as non-uniform subdivision, tight bounding volumes, and overlapping children.

In addition, there is a simple `KeyPartitioner` available in case you bring your own partitioning scheme (see example below).

### An example: Octree partitioning

In the following example, we load 3D data, and we re-partition it with the octree partitioning (note that in practce you can have metadata in addition to the 3 coordinates):

```python
from pyspark3d.repartitioning import addSPartitioning
from pyspark3d.repartitioning import repartitionByCol

# Load data
df = spark.read.format("fits")\
	.option("hdu", 1)\
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

# Specify options.
options = {
	"geometry": "points",
	"colnames": "x,y,z",
	"coordSys": "cartesian",
	"gridtype": "octree"}

# Add a column containing the future partition ID.
# Note that no shuffle has been done yet.
df_colid = addSPartitioning(df, options, numPartitions=8)

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

# Trigger the repartition
df_repart = repartitionByCol(df_colid, "partition_id", 8)

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

### Options for repartitioning

...

### Your own Partitioning

...

## Memory and speed considerations

We advice to cache the re-partitioned sets, to speed-up future call by not performing the re-partitioning again.
However keep in mind that if a large `numPartitions` decreases the cost of performing future queries (cross-match, KNN, ...), it increases the partitioning cost as more partitions implies more data shuffle between partitions. There is no magic number for `numPartitions ` which applies in general, and you'll need to set it according to the needs of your problem.
