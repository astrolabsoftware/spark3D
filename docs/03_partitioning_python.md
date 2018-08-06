---
permalink: /docs/partitioning/python/
layout: splash
title: "Tutorial: Partition your space"
date: 2018-06-18 22:31:13 +0200
---

# Tutorial: Partition your space

## Why partitioning the space?

Often the data is not written on disks the way we would like to have it distributed. Re-partitioning the space is a way to re-organise the data to speed-up later queries and explorations of the data set.

Unfortunately, re-partitioning the space involves potentially large shuffle between executors which have to send and receive data according to the new wanted scheme. Depending on the network capacity and the size of the initial data set, this operation can be costly and the re-partition is only a good idea if multiple queries have to be performed.

## Available partitioning and partitioners

TBD
