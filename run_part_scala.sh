#!/bin/bash

SBT_VERSION_SPARK=2.11
VERSION=0.3.1

# Compile the code
sbt ++2.11.8 package

PACK=com.github.astrolabsoftware:spark-fits_2.11:0.7.1
SF=target/scala-2.11/spark3d_2.11-${VERSION}.jar
HP=lib/jhealpix.jar

MASTERURL=
fitsfn=

# "order" "col" "range" "rep"
for method in "range" "col"
do
    spark-submit \
        --master ${MASTERURL} \
        --driver-memory 4g --executor-memory 30g --executor-cores 17 --total-executor-cores 102 \
        --jars ${SF},${HP} --packages ${PACK} \
        --class com.astrolabsoftware.spark3d.examples.PartitioningDF \
        target/scala-${SBT_VERSION_SPARK}/spark3d_${SBT_VERSION_SPARK}-${VERSION}.jar \
        $fitsfn 1 "Z_COSMO,RA,DEC" true "onion" 102 ${method}
done

