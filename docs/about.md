---
layout: posts
title: About
permalink: /about/
author_profile: true
---

<p align="center"><img width="500" src="https://github.com/astrolabsoftware/spark3D/raw/master/pic/spark3d_newapi.png"/>
</p>

spark3D should be viewed as an extension of the Apache Spark framework, and more specifically the Spark SQL module, focusing on the manipulation of three-dimensional data sets.

## Why spark3D?

The volume of data recorded by current and
future High Energy Physics & Astrophysics experiments for example,
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
[spark3D](https://github.com/astrolabsoftware/spark3D) extends Apache Spark to
efficiently load, process, and analyze large-scale 3D spatial data sets across machines.

## More specifically?

Why would you use spark3D? If you often need to repartition large spatial 3D data sets, or perform spatial queries (neighbour search, window queries, cross-match, clustering, ...), spark3D is for you. It contains optimised classes and methods to do so, and it spares you the implementation time! In addition, a big advantage of all those extensions is to efficiently perform visualisation of large data sets by quickly building a representation of your data set (see more [here](https://astrolabsoftware.github.io/spark3D/)).

## How spark3D is organised?

spark3D exposes two API: Scala (spark3D) and Python (pyspark3d). The core developments are done in Scala, and interfaced with Python using the great [py4j](https://www.py4j.org/) package. This means pyspark3d might not contain all the features present in spark3D.
In addition, due to difference between Scala and Python, there might be subtle differences in the two APIs.

While we try to stick to the latest Apache Spark developments, spark3D started with the RDD API and slowly migrated to use the DataFrame API. This process left a huge imprint on the code structure, and low-level layers in spark3D often still use RDD to manipulate the data. Do not be surprised if things are moving, the package is under an active development but we try to keep the user interface as stable as possible!

Last but not least: spark3D is by no means complete, and you are welcome to suggest changes, report bugs or inconsistent implementations, and contribute directly to the package!

*Why 3? Because there are already plenty of very good packages dealing with 2D data sets (e.g. [geospark](http://geospark.datasyslab.org/), [geomesa](https://www.geomesa.org/), [magellan](https://magellan.ghost.io/), [GeoTrellis](https://github.com/locationtech/geotrellis), and others), but that was not suitable for many applications such as in astronomy!*

## Support

![LAL]({{ "/assets/images/lal_logo.jpg" | absolute_url }})
![UPSUD]({{ "/assets/images/Logo_Université_Paris-Saclay.svg.png" | absolute_url }})
![CNRS]({{ "/assets/images/1012px-Centre_national_de_la_recherche_scientifique.svg.png" | absolute_url }})
