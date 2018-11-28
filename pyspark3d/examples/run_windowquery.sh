#!/bin/bash

# Raw
fn="../../src/test/resources/cartesian_spheres_manual.csv"

spark-submit --jars "../../target/scala-2.11/spark3D-assembly-0.3.0.jar" \
  windowquery.py -inputpath ${fn} \
  -hdu 1 -windowtype sphere -windowcoord 2.0 1.0 2.0 1.8 --cartesian
