#!/bin/bash

# Raw
spark-submit --jars "../../target/scala-2.11/spark3D-assembly-0.3.0.jar" \
  knn.py -inputpath "../../src/test/resources/cartesian_spheres.fits" \
  -hdu 1 --cartesian

# With Octree
spark-submit --jars "../../target/scala-2.11/spark3D-assembly-0.3.0.jar" \
  knn.py -inputpath "../../src/test/resources/cartesian_spheres.fits" \
  -hdu 1 -part "octree" -npart 10 --cartesian
