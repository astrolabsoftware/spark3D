![logo](https://github.com/JulienPeloton/spark3D/blob/master/pic/spark3d_logo.jpg)

  spark3D
  ------------------------------------------------------------------------------------------------------------------

[![Build Status](https://travis-ci.org/JulienPeloton/spark3D.svg?branch=geometryObjects)](https://travis-ci.org/JulienPeloton/spark3D)
[![codecov](https://codecov.io/gh/JulienPeloton/spark3D/branch/geometryObjects/graph/badge.svg)](https://codecov.io/gh/JulienPeloton/spark3D)

The package is under an active development! Here is the short term development list:

- While GeoSpark relies on the package JTS for 2D objects, there is no support for 3D objects in JTS, so we have to develop them ourselves.
- Upgrade 2D RDD to 3D RDD (circle -> sphere, parallelogram -> parallelepiped, and so on).
- Upgrade 2D methods for partitioning/join/query/indexing to 3D.

Why spark3D?
================

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

Contributors
================

* Julien Peloton (peloton at lal.in2p3.fr)
* Christian Arnault (arnault at lal.in2p3.fr)
* Mayur Bhosale (mayurdb31 at gmail.com) -- GSoC 2018.

Goals of Spark3D
=============================================================
We have to distribute the computation for a very large amount of 2D or 3D data,
eventually exceeding the memory available in our cluster (The Spark cluster).
Several goals have to be undertaken in this project:

- construct a highly scalable architecture so as to accept very large dataset. (typically greater than what can be handled by practical memory sets)
- datasets are sets of 3D data, ie. object data containing both 3D information plus additional related data.
- mechanisms should offer:
  + indexing mechanisms
  + ways to define a metric (ie. distance between objects)
  + selection capability of objects or objects within a region
- work with as many input file format as possible (CSV, JSON, FITS, and so on)
- package the developments into an open-source library.

Data storage and distribution across machines
============

Since the scientific domain considered here is mostly the Astrophysics domain,
the natural storage or exchange file format is the FITS format.
Therefore we consider as part of the problem, the possibility to allow FITS files
to be directly injected into the HDFS infrastructure,
so as to develop a Spark based applications.

The usual [cfitsio](https://heasarc.gsfc.nasa.gov/fitsio/fitsio.html) library,
as well as the FITS I/O format are not adapted to a distributed file system as HDFS.

Therefore we will have to develop low level Reader/Writer services,
to support direct access to FITS data, without copy nor conversion needs.

To tackle this challenge, we started a new project called
[spark-fits](https://github.com/JulienPeloton/spark-fits), which provides a
Spark connector for FITS data, and a Scala library for manipulating FITS file.

Use case for large astronomical data sets with Apache Spark
============

TBD
