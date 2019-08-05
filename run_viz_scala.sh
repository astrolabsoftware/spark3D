#!/bin/bash

SCAlA_VERSION=2.11
VERSION=0.3.1

# Compile the code
sbt ++2.11.8 package

PACK=com.github.astrolabsoftware:spark-fits_2.11:0.7.1
SF=target/scala-2.11/spark3d_2.11-${VERSION}.jar
HP=lib/jhealpix.jar,lib/plotly-assembly-0.3.0-SNAPSHOT.jar

MASTERURL=yarn
inputfn="LSST1Y/out_srcs_s1_0.fits"

spark-submit \
    --master ${MASTERURL} \
    --driver-memory 4g --executor-memory 28g --executor-cores 17 --total-executor-cores 102 \
    --jars ${SF},${HP} --packages ${PACK} \
    --class com.astrolabsoftware.spark3d.examples.VisualizePart \
    target/scala-${SCAlA_VERSION}/spark3d_${SCAlA_VERSION}-${VERSION}.jar \
    $inputfn "Z_COSMO,RA,DEC" true "onion" 8 0.0001 "username" "api_key"

inputfn="dc2"
spark-submit \
    --master ${MASTERURL} \
    --driver-memory 4g --executor-memory 28g --executor-cores 17 --total-executor-cores 102 \
    --jars ${SP},${HP} --packages ${PACK} \
    --class com.astrolabsoftware.spark3d.examples.VisualizePart \
    target/scala-${SCAlA_VERSION}/spark3d_${SCAlA_VERSION}-${VERSION}.jar \
    $inputfn "position_x_mock,position_y_mock,position_z_mock" false "octree" 512 0.001 "username" "api_key"
