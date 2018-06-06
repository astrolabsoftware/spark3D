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

// spark3d lib
import com.spark3d.utils.GridType
import com.spark3d.spatial3DRDD.Point3DRDDFromFITS
import com.spark3d.spatial3DRDD.Point3DRDDFromCSV
import com.spark3d.spatialPartitioning.SpatialPartitioner
import com.spark3d.spatialOperator.PixelCrossMatch
import com.spark3d.serialization.Spark3dConf.spark3dConf

// Spark lib
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

// Logger
import org.apache.log4j.Level
import org.apache.log4j.Logger

object CrossMatch {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // Initialise our spark connector
  val conf = spark3dConf
  
  val spark = SparkSession
    .builder()
    .appName("CrossMatch")
    .config(conf)
    .getOrCreate()

  /**
    * Main
    */
  def main(args : Array[String]) = {

    // Data files
    val fnA_fits = args(0).toString
    val fnB_fits = args(1).toString

    // HDU index
    val hdu = args(2).toInt

    // Columns to load
    val columns = args(3).toString

    // Number of partitions
    val nPart = args(4).toInt

    // Resolution of the underlying map
    val nside = args(5).toInt

    // Load the data as Point3DRDD
    val pointRDDA = new Point3DRDDFromFITS(spark, fnA_fits, hdu, columns, true)
    val pointRDDB = new Point3DRDDFromFITS(spark, fnB_fits, hdu, columns, true)
    // val pointRDDA = new Point3DRDDFromCSV(spark, fnA_fits, columns, true)
    // val pointRDDB = new Point3DRDDFromCSV(spark, fnB_fits, columns, true)

    // Re-partition the space
    val pointRDD_partA = pointRDDA.spatialPartitioning(GridType.LINEARONIONGRID, nPart).cache()
    val partitionerA = pointRDD_partA.partitioner.get.asInstanceOf[SpatialPartitioner]
    val pointRDD_partB = pointRDDB.spatialPartitioning(partitionerA).cache()

    // val xMatchH = PixelCrossMatch.CrossMatchHealpixIndex(
    //   pointRDD_partA, pointRDD_partB, nside, "healpix")
    // val xMatchA = PixelCrossMatch.CrossMatchHealpixIndex(
    //   pointRDD_partA, pointRDD_partB, nside, "A")
    // // Keeping only elements from B with counterpart in A
    // val xMatchB = PixelCrossMatch.CrossMatchHealpixIndex(
    //   pointRDD_partA, pointRDD_partB, nside, "B")
    // // Keeping all elements with counterparts in both A and B
    val xMatchAB = PixelCrossMatch.CrossMatchHealpixIndex(
      pointRDD_partA, pointRDD_partB, nside, "AB")

    for (it <- 0 to 1) {
      println("iteration: ", it)
      println(xMatchAB.count())
      // println("Keeping only elements from A with counterpart in B: ", xMatchA.count(), " points")
      // println("Keeping only elements from B with counterpart in A: ", xMatchB.count(), " points")
      // println("Keeping all elements with counterparts in both A and B: ", xMatchAB.count(), " points")
    }
  }
}
