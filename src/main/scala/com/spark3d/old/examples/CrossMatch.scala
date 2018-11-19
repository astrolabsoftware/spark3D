// /*
//  * Copyright 2018 Julien Peloton
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// package com.astrolabsoftware.spark3d.examples
//
// // spark3d lib
// import com.astrolabsoftware.spark3d.utils.GridType
// import com.astrolabsoftware.spark3d.spatial3DRDD.Point3DRDD
// import com.astrolabsoftware.spark3d.spatialPartitioning.SpatialPartitioner
// import com.astrolabsoftware.spark3d.spatialOperator.PixelCrossMatch
// import com.astrolabsoftware.spark3d.serialization.Spark3dConf.spark3dConf
//
// // Spark lib
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.functions._
// import org.apache.spark.rdd.RDD
// import org.apache.spark.SparkConf
//
// // Logger
// import org.apache.log4j.Level
// import org.apache.log4j.Logger
//
// /**
//   * Perform the Xmatch of 2 sets A and B using the LINEARONIONGRID, with
//   * a healpix grid on top.
//   */
// object CrossMatch {
//   // Set to Level.WARN is you want verbosity
//   Logger.getLogger("org").setLevel(Level.WARN)
//   Logger.getLogger("akka").setLevel(Level.WARN)
//
//   // Initialise our spark connector with spark3d configuration
//   val conf = spark3dConf
//
//   val spark = SparkSession
//     .builder()
//     .appName("CrossMatch")
//     .config(conf)
//     .getOrCreate()
//
//   /**
//     * Perform the Xmatch of 2 sets A and B using the LINEARONIONGRID, with
//     * a healpix grid on top.
//     *
//     * @param fnA : (String)
//     *   Input FITS file for set A
//     * @param fnB : (String)
//     *   Input FITS file for set B
//     * @param hdu : (Int)
//     *   HDU index to read
//     * @param columns : (String)
//     *   Column names (comma-separated) to load
//     * @param nPart : (Int)
//     *   Number of final partitions
//     * @param nside : (Int)
//     *   Resolution for each shell
//     * @param kind : (String)
//     *   Kind of Xmatch: A, B, AB, or healpix
//     */
//   def main(args : Array[String]) = {
//
//     // Data files
//     val fnA_fits = args(0).toString
//     val fnB_fits = args(1).toString
//
//     // HDU index
//     val hdu = args(2).toString
//
//     // Columns to load
//     val columns = args(3).toString
//
//     // Number of partitions
//     val nPart = args(4).toInt
//
//     // Resolution of the underlying map
//     val nside = args(5).toInt
//
//     // Kind of Xmatch to perform: A, B, AB, or healpix
//     val kind = args(6).toString
//
//     // Load the data as Point3DRDD
//     val options = Map("hdu" -> hdu)
//     val pointRDDA = new Point3DRDD(spark, fnA_fits, columns, true, "fits", options)
//     val pointRDDB = new Point3DRDD(spark, fnB_fits, columns, true, "fits", options)
//
//     // Re-partition the space and cache the result
//     val pointRDD_partA = pointRDDA.spatialPartitioning(
//       GridType.LINEARONIONGRID, nPart).cache()
//     val partitionerA = pointRDD_partA.partitioner.get.asInstanceOf[SpatialPartitioner]
//     val pointRDD_partB = pointRDDB.spatialPartitioning(partitionerA).cache()
//
//     // Xmatch
//     val xMatchAB = PixelCrossMatch.CrossMatchHealpixIndex(
//       pointRDD_partA, pointRDD_partB, nside, kind)
//
//     // Do it several times to see the improvement
//     for (it <- 0 to 2) {
//       println("iteration: ", it)
//       println(kind, xMatchAB.count())
//     }
//   }
// }
