## 0.1.1

- Add scripts to generate test data (#34)
- Added Octree Partitioned RDD support (#36)
- RDD[Sphere] added (`SphereRDD`) (#38)
- Code refactoring: Replace Sphere by ShellEnvelope, Shape3D heritage, and move everything under geometryObjects (#40)
- Add very simple Kryo registration for spark3D classes. Probably need more (#31, #28).
- Few fixes here and there...

## $\leq$ 0.1.0

Repo on fire! See from this #33 and earlier commits. In short, what was available at that time (including bugs!):

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