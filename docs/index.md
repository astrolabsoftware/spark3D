# Welcome to spark3D!

[![Build Status](https://travis-ci.org/JulienPeloton/spark3D.svg?branch=master)](https://travis-ci.org/JulienPeloton/spark3D)
[![codecov](https://codecov.io/gh/JulienPeloton/spark3D/branch/master/graph/badge.svg)](https://codecov.io/gh/JulienPeloton/spark3D)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.JulienPeloton/spark3d_2.11/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.JulienPeloton/spark3d_2.11)

-

[Home]({{ site.baseurl }}{% link index.md %}) / [Tutorial Installation]({{ site.baseurl }}{% link tutorial_insta.md %}) / [Tutorial Intro]({{ site.baseurl }}{% link tutorial_intro.md %}) / [Tutorial Partitioning]({{ site.baseurl }}{% link tutorial_parti.md %}) / [Tutorial Query]({{ site.baseurl }}{% link tutorial_query.md %}) / [Tutorial Visualisation]({{ site.baseurl }}{% link tutorial_visua.md %})

## Latest News

- [05/2018] **GSoC 2018**: spark3D has been selected to the Google Summer of Code (GSoC) 2018. Congratulation to [@mayurdb](https://github.com/mayurdb) who will work on the project this year!
- [06/2018] **Release**: version 0.1.0, 0.1.1

## Why spark3D?

The volume of data recorded by current and
future High Energy Physics & Astrophysics experiments,
and the complexity of their data set require a broad panel of
knowledge in computer science, signal processing, statistics, and physics.
Exact analysis of those data sets produced is a serious computational challenge,
which cannot be done without the help of state-of-the-art tools.
This has to be matched by sophisticated and robust analysis performed on many
powerful machines, as we need to process or simulate several times data sets.

While a lot of efforts have been made to develop cluster computing systems for
processing large-scale spatial 2D data
(see e.g. [GeoSpark](http://geospark.datasyslab.org)),
there are very few frameworks to process and analyse 3D data sets
which were hitherto too costly to be processed efficiently.
With the recent development of fast and general engine such as
[Apache Spark](http://spark.apache.org), taking advantage of
distributed systems, we enter in a new area of possibilities.
[spark3D](https://github.com/JulienPeloton/spark3D) extends Apache Spark to
efficiently load, process, and analyze large-scale 3D spatial data sets across machines.

### Goals of spark3D

We have to distribute the computation for a very large amount of 3D data.
Several goals have to be undertaken in this project:

- construct a highly scalable architecture so as to accept very large dataset. (typically greater than what can be handled by practical memory sets)
- datasets are sets of 3D data, ie. object data containing both 3D information plus additional related data.
- mechanisms should offer:
  + indexing mechanisms
  + ways to define a metric (ie. distance between objects)
  + selection capability of objects or objects within a region
- work with as many input file format as possible (CSV, JSON, FITS, and so on)
- package the developments into an open-source library.

### Current spark3D package

The current spark3D package provides

- Read and format data from external data sets. Coordinates can be spherical or cartesian.
  - *Currently available: FITS and CSV data format.*
- Instantiate 3D objects.
  - *Currently available: Point, Sphere, Spherical shell, Box.*
- Create RDD[T] from a raw RDD whose T is a 3D object. The new RDD has the same partitioning as the raw RDD.
  - *Currently available: RDD[Point], RDD[Sphere]*
- Re-partition RDD[T].
  - *Currently available: Onion grid, Octree.*
- Identification and join methods between two data sets.
  - *Currently available: cross-match between two RDD.*

What we are thinking at

- RangeQuery and KNN methods?
- KB Tree?
- [Project issues](https://github.com/JulienPeloton/spark3D/issues)
- ?
