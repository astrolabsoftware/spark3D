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
