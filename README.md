[![Build Status](https://travis-ci.org/astrolabsoftware/spark3D.svg?branch=master)](https://travis-ci.org/astrolabsoftware/spark3D)
[![codecov](https://codecov.io/gh/astrolabsoftware/spark3D/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/spark3D)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.astrolabsoftware/spark3d_2.11/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.astrolabsoftware/spark3d_2.11)

**The package is under an active development!**

## Latest News

- [05/2018] **GSoC 2018**: spark3D has been selected to the Google Summer of Code (GSoC) 2018. Congratulation to [@mayurdb](https://github.com/mayurdb) who will work on the project this year!
- [06/2018] **Release**: version 0.1.0, 0.1.1
- [07/2018] **New location**: spark3D is an official project of [AstroLab Software](https://astrolabsoftware.github.io/)!
- [07/2018] **Release**: version 0.1.3, 0.1.4, 0.1.5
- [08/2018] **Release**: version 0.2.0, 0.2.1 (pyspark3d)
- [09/2018] **Release**: version 0.2.2

<p align="center"><img width="500" src="https://github.com/astrolabsoftware/spark3D/raw/master/pic/spark3d_newapi.png"/>
</p>

## Rationale

spark3D should be viewed as an extension of the Apache Spark framework, and more specifically the Spark SQL module, focusing on the manipulation of three*-dimensional data sets.

Why would you use spark3D? If you often need to repartition large spatial 3D data sets, or perform spatial queries (neighbour search, window queries, cross-match, clustering, ...), spark3D is for you. It contains optimised classes and methods to do so, and it spares you the implementation time! In addition, a big advantage of all those extensions is to efficiently perform visualisation of large data sets by quickly building a representation of your data set (see more [here](https://astrolabsoftware.github.io/spark3D/)).

spark3D exposes two API: Scala (spark3D) and Python (pyspark3d). The core developments are done in Scala, and interfaced with Python using the great [py4j](https://www.py4j.org/) package. This means pyspark3d might not contain all the features present in the spark3D.
In addition, due to different in Scala and Python, there might be subtle difference in the two APIs.

While we try to stick to the latest Apache Spark developments, spark3D started with the RDD API and slowly migrated to use the DataFrame API. This process left a huge imprint on the code structure, and low-level layers in spark3D often still use RDD to manipulate the data. Do not be surprised if things are moving, the package is under an active development but we try to keep the user interface as stable as possible!

Last but not least: spark3D is by no means complete, and you are welcome to suggest changes, report bugs or inconsistent implementations, and contribute directly to the package!

Cheers,
Julien

*Why 3? Because there are already plenty of very good packages dealing with 2D data sets (e.g. [geospark](http://geospark.datasyslab.org/), [geomesa](https://www.geomesa.org/), [magellan](https://magellan.ghost.io/), [GeoTrellis](https://github.com/locationtech/geotrellis), and others), but that was not suitable for many applications such as in astronomy!*

## Installation and tutorials

### Scala

You can link spark3D to your project (either spark-shell or spark-submit) by specifying the coordinates:

```
spark-submit --packages "com.github.astrolabsoftware:spark3d_2.11:0.3.0"
```

### Python

Just run

```bash
pip install pyspark3d
```

Note that we release the assembly JAR with it.

### More information

See our [website](https://astrolabsoftware.github.io/spark3D/)!

## Contributors

* Julien Peloton (peloton at lal.in2p3.fr)
* Christian Arnault (arnault at lal.in2p3.fr)
* Mayur Bhosale (mayurdb31 at gmail.com) -- GSoC 2018.

Contributing to spark3D: see [CONTRIBUTING](https://github.com/astrolabsoftware/spark3D/blob/master/CONTRIBUTING.md).

## Support

<p align="center"><img width="100" src="https://github.com/astrolabsoftware/spark-fits/raw/master/pic/lal_logo.jpg"/> <img width="100" src="https://github.com/astrolabsoftware/spark-fits/raw/master/pic/psud.png"/> <img width="100" src="https://github.com/astrolabsoftware/spark-fits/raw/master/pic/1012px-Centre_national_de_la_recherche_scientifique.svg.png"/></p>
