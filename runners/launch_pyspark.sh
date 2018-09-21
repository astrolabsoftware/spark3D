#!/bin/bash

# Run `sbt 'set test in assembly := {}' ++2.11.8 assembly` to get the JAR
JARS=../target/scala-2.11/spark3D-assembly-0.2.1.jar

# Octree - no part
 spark-submit --jars $JARS \
   ../examples/python/collapse_functions.py \
   -inputpath "/Users/julien/Documents/workspace/data/nbody/velocity_field_20_flat.fits" \
   -hdu 1 -part OCTREE -npart 10000 --plot

exit

## Other pyspark scripts -- remove the exit above.

# Onion - no part
spark-submit --jars $JARS \
  ../examples/python/pyspark3d_onion_part.py \
  -inputpath "../src/test/resources/astro_obs.fits" \
  -hdu 1 -npart 10 --plot

# Onion
spark-submit --jars $JARS \
  ../examples/python/pyspark3d_onion_part.py \
  -inputpath "../src/test/resources/astro_obs.fits" \
  -hdu 1 -part LINEARONIONGRID -npart 10 --plot

# Octree - no part
spark-submit --jars $JARS \
  ../examples/python/pyspark3d_octree_part.py \
  -inputpath "../src/test/resources/cartesian_spheres.fits" \
  -hdu 1 -npart 10 --plot

# Octree
spark-submit --jars $JARS \
  ../examples/python/pyspark3d_octree_part.py \
  -inputpath "../src/test/resources/cartesian_spheres.fits" \
  -hdu 1 -part OCTREE -npart 10 --plot
