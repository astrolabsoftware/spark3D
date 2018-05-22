/*
 * Copyright 2018 Julien Peloton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spark3d.examples

import com.spark3d.utils.GridType
import com.spark3d.spatial3DRDD.Point3DRDD.Point3DRDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

object OnionSpace {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession
    .builder()
    .appName("OnionSpace")
    .getOrCreate()

  def main(args : Array[String]) = {

    val fn_fits = args(0).toString
    val pointRDD = new Point3DRDD(spark, fn_fits, args(1).toInt, args(2).toString)
    val partitionsBefore = pointRDD.rawRDD.mapPartitions(
      iter => Array(iter.size).iterator, true).collect()

    // Partition my space
    val pointRDD_part = pointRDD.spatialPartitioning(GridType.LINEARONIONGRID, 10)

    // Collect the size of each partition
    val partitionsAfter = pointRDD_part.mapPartitions(
      iter => Array(iter.size).iterator, true).collect()

    println("Before: ", partitionsBefore.toList)
    println("After : ", partitionsAfter.toList)
  }
}
