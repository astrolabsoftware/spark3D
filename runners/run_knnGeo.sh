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
VERSION=0.1.5

# Package it
sbt ++${SBT_VERSION} package

# Parameters (put your file)
fitsfn="hdfs://134.158.75.222:8020//lsst/LSST1Y/out_srcs_s1_0.fits"
#fitsfn="hdfs://134.158.75.222:8020//user/julien.peloton/sph_point_1000000.fits"
fitsfn="hdfs://134.158.75.222:8020//user/julien.peloton/point_e7.csv"

nNeighbours=500

## Dependencies
jars="lib/jhealpix.jar"
packages="com.github.JulienPeloton:spark-fits_2.11:0.4.0,org.datasyslab:geospark:1.1.3"

# Run it!
spark-submit \
  --master spark://134.158.75.222:7077 \
  --driver-memory 15g --executor-memory 29g --executor-cores 17 --total-executor-cores 102 \
  --jars ${jars} \
  --class com.spark3d.examples.knnTestGeo \
  --conf "spark.kryoserializer.buffer.max=1024" \
  --packages ${packages} \
  target/scala-${SBT_VERSION_SPARK}/spark3d_${SBT_VERSION_SPARK}-${VERSION}.jar \
  $fitsfn $nNeighbours
