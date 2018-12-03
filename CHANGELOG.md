## 0.3.1

- Introducing KNN + DataFrame API ([PR](https://github.com/astrolabsoftware/spark3D/pull/110))
- Fix wrong ordering when using unique in KNN ([PR](https://github.com/astrolabsoftware/spark3D/pull/111))
- Introducing Window Query + DataFrame API ([PR](https://github.com/astrolabsoftware/spark3D/pull/112))
- Rename addSPartitioning -> prePartition ([PR](https://github.com/astrolabsoftware/spark3D/pull/113))
- Accelerated partitioning (aborted and postponed) ([PR](https://github.com/astrolabsoftware/spark3D/pull/114))

## 0.3.0

- Complete change of the interface ([PR](https://github.com/astrolabsoftware/spark3D/pull/108)). This includes:
  - DataFrame API
  - Extension of the Spark SQL module (new methods + implicits on DataFrame)
  - Metadata are propagated (not just 3D coordinates).
  - Seamless interface with Python
- Compiling against Apache Spark 2.3.2: fix incompatibilities ([PR](https://github.com/astrolabsoftware/spark3D/pull/107))
- Website update: spark-fits version + vulnerability fix ([PR](https://github.com/astrolabsoftware/spark3D/pull/105))
-  Fix wrong angle definition in cartesian to spherical coordinate change! ([PR](https://github.com/astrolabsoftware/spark3D/pull/102))


## 0.2.2

- Add the conversion from FITS file (single HDU) to parquet ([PR](https://github.com/astrolabsoftware/spark3D/pull/92))
- Tools to perform live 3D RDD visualisation (in progress) ([PR](https://github.com/astrolabsoftware/spark3D/pull/93))
- Several updates to the Travis CI  ([PR](https://github.com/astrolabsoftware/spark3D/pull/96))
- Add example dealing with collapse function in pyspark ([PR](https://github.com/astrolabsoftware/spark3D/pull/97))
- Deploy pyspark3d with pip ([PR](https://github.com/astrolabsoftware/spark3D/pull/98))
- Clarify the use of get_spark_session and load_user_conf (for tests only!) ([PR](https://github.com/astrolabsoftware/spark3D/pull/99))

## 0.2.1

- pyspark3d contains all the features of spark3D ([partitioning](https://github.com/astrolabsoftware/spark3D/pull/89), [operators](https://github.com/astrolabsoftware/spark3D/pull/90))
- Scala reflection support for python using py4j ([PR](https://github.com/astrolabsoftware/spark3D/pull/90))

## 0.2.0

- Add support for Python: pyspark3d ([PR](https://github.com/astrolabsoftware/spark3D/pull/86)).
- Update the Travis script to run test suites for both scala and python ([PR](https://github.com/astrolabsoftware/spark3D/pull/86)).
- Switch to docker-based travis to test against ubuntu 16.04 ([PR](https://github.com/astrolabsoftware/spark3D/pull/86)).

## 0.1.5

- Allow user to cache the rawRDD while loading data to speed-up re-partitioning. ([PR](https://github.com/astrolabsoftware/spark3D/pull/81))
- Script to benchmark the partitioning ([PR](https://github.com/astrolabsoftware/spark3D/pull/72))
- Several minor fix ([PR](https://github.com/astrolabsoftware/spark3D/pull/71), [PR](https://github.com/astrolabsoftware/spark3D/pull/76), [PR](https://github.com/astrolabsoftware/spark3D/pull/80))

## 0.1.4

- Unify the IO: One constructor to rule them all! ([PR](https://github.com/astrolabsoftware/spark3D/pull/69))
- Octree and Geometry Objects Bug Fixes ([PR](https://github.com/astrolabsoftware/spark3D/pull/67))

## 0.1.3

- Add KNN routines ([KNN](https://github.com/astrolabsoftware/spark3D/pull/59), [KNN](https://github.com/astrolabsoftware/spark3D/pull/60), [KNN](https://github.com/astrolabsoftware/spark3D/pull/62))
- Unify API to load data ([Point3DRDD](https://github.com/astrolabsoftware/spark3D/pull/63), [SphereRDD](https://github.com/astrolabsoftware/spark3D/pull/64))
- Speed-up cross-match methods by using native Scala methods ([Scala](https://github.com/astrolabsoftware/spark3D/pull/58))
- Add a new website + spark3D belongs to AstroLab Software ([website](https://astrolabsoftware.github.io/))
- Update tutorials ([tuto](https://astrolabsoftware.github.io/spark3D/).
- Few fixes here and there...

## 0.1.1

- Add scripts to generate test data ([PR](https://github.com/astrolabsoftware/spark3D/pull/34))
- Added Octree Partitioned RDD support ([PR](https://github.com/astrolabsoftware/spark3D/pull/36))
- RDD[Sphere] added (`SphereRDD`) ([PR](https://github.com/astrolabsoftware/spark3D/pull/38))
- Code refactoring: Replace Sphere by ShellEnvelope, Shape3D heritage, and move everything under geometryObjects ([PR](https://github.com/astrolabsoftware/spark3D/pull/40))
- Add very simple Kryo registration for spark3D classes. Probably need more ([PR](https://github.com/astrolabsoftware/spark3D/pull/31), [PR](https://github.com/astrolabsoftware/spark3D/pull/28)).
- Few fixes here and there...

## <= 0.1.0

Repo on fire! See from this [PR](https://github.com/astrolabsoftware/spark3D/pull/33) and earlier commits. In short, what was available at that time (including bugs!):

- Read and format data from external data sets. Coordinates can be spherical or cartesian.
  - *Currently available: FITS and CSV data format.*
- Instantiate 3D objects.
  - *Currently available: Point, Sphere, Spherical shell, Box.*
- Create RDD[T] from a raw RDD whose T is a 3D object. The new RDD has the same partitioning as the raw RDD.
  - *Currently available: RDD[Point]*
- Re-partition RDD[T].
  - *Currently available: Onion grid, Octree.*
- Identification and join methods between two data sets.
  - *Currently available: cross-match between two RDD.*
