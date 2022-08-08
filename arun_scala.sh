#!/bin/bash

SBT_VERSION_SPARK=2.11
VERSION=0.3.1

# Compile the code
sbt ++2.11.8 package

PACK=com.github.astrolabsoftware:spark-fits_2.11:0.7.1
SF=target/scala-2.11/spark3d_2.11-${VERSION}.jar
HP=lib/jhealpix.jar

#MASTERURL=spark://134.158.75.222:7077
MASTERURL=yarn
 # fitsfn=hdfs://134.158.75.222:8020//lsst/LSST1Y/out_srcs_s1_3.fits
# fitsfn=hdfs://134.158.75.222:8020/user/alia/a.fits
 fitsfn=hdfs://134.158.75.222:8020/user/julien.peloton/dc2

#This is used in evaluation of the KDtree partitioner
sum=0
for i in {1..10}
do 
    echo "Run number is $i"
    start_time="$(date -u +%s)"
    spark-submit \
        --master ${MASTERURL} \
        --driver-memory 4g --executor-memory 28g --executor-cores 17 --total-executor-cores 102 \
        --jars ${SF},${HP} --packages ${PACK} \
        --class com.astrolabsoftware.spark3d.examples.Test \
        target/scala-${SBT_VERSION_SPARK}/spark3d_${SBT_VERSION_SPARK}-${VERSION}.jar \
        $fitsfn 1 "position_x_mock,position_y_mock,position_z_mock" true "kdtree" 8 "col"
    end_time="$(date -u +%s)"
    elapsed="$(($end_time-$start_time))"
    echo "Total of $elapsed seconds elapsed for process"
    sum="$(($sum+$elapsed))"
    wait
done
avgT="$(($sum/10))"
echo "average elapsed time is  $avgT "


