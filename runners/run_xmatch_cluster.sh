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
fitsfnA="hdfs://134.158.75.222:8020//lsst/LSST1Y/out_srcs_s1_0.fits"
fitsfnB="hdfs://134.158.75.222:8020//lsst/LSST1Y/out_srcs_s1_1.fits"

hdu=1
columns="Z_COSMO,RA,Dec"
nPart=100
nside=512

kind="healpix"

## Dependencies
jars="lib/jhealpix.jar"
packages="com.github.astrolabsoftware:spark-fits_2.11:0.6.0"

# Run it!
spark-submit \
  --master spark://134.158.75.222:7077 \
  --driver-memory 4g --executor-memory 30g --executor-cores 17 --total-executor-cores 102 \
  --jars ${jars} \
  --class com.spark3d.examples.CrossMatch \
  --packages ${packages} \
  target/scala-${SBT_VERSION_SPARK}/spark3d_${SBT_VERSION_SPARK}-${VERSION}.jar \
  $fitsfnA $fitsfnB $hdu $columns $nPart $nside
