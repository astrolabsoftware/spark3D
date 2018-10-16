#!/bin/bash
# Copyright 2018 Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## SBT Version
SBT_VERSION=2.11.8
SBT_VERSION_SPARK=2.11

## Package version
VERSION=0.2.2

# Package it
sbt ++${SBT_VERSION} package

# Parameters (put your file)
fitsfn="src/test/resources/event000001000-hits.csv"
columns="x,y,z"
isSpherical=false

#fitsfn="hdfs://134.158.75.222:8020//lsst/LSST1Y/out_srcs_s1_0.fits"
#fitsfn="hdfs://134.158.75.222:8020//lsst/LSST1Y"
#hdu=1
#columns="Z_COSMO,RA,Dec"
#isSpherical=true

nNeighbours=5000

## Dependencies
jars="lib/jhealpix.jar"
packages="com.github.astrolabsoftware:spark-fits_2.11:0.7.0,com.github.haifengl:smile-core:1.5.1,com.github.haifengl:smile-plot:1.5.1,com.github.haifengl:smile-math:1.5.1,com.github.haifengl:smile-scala_2.11:1.5.1"

# Run it!
spark-submit \
  --master local[*] \
  --driver-memory 4g --executor-memory 4g \
  --jars ${jars} \
  --class com.spark3d.examples.knnTestPlot \
  --conf "spark.kryoserializer.buffer.max=1024" \
  --packages ${packages} \
  target/scala-${SBT_VERSION_SPARK}/spark3d_${SBT_VERSION_SPARK}-${VERSION}.jar \
  $fitsfn $columns $isSpherical $nNeighbours
