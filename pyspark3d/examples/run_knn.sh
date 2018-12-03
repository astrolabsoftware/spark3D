#!/bin/bash

# Raw
fn="../../src/test/resources/cartesian_spheres.fits"
fn="../../src/test/resources/cartesian_spheres_manual_knn.csv"

spark-submit --jars "../../target/scala-2.11/spark3D-assembly-0.3.1.jar" \
  knn.py -inputpath ${fn} \
  -hdu 1 -K 2 -target 0.2 0.2 0.2 --cartesian

# # With Octree
spark-submit --jars "../../target/scala-2.11/spark3D-assembly-0.3.1.jar" \
  knn.py -inputpath ${fn} \
  -hdu 1 -part "octree" -npart 10 -K 2 -target 0.2 0.2 0.2 --cartesian
