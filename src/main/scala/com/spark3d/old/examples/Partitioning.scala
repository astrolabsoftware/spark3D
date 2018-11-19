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
// import com.astrolabsoftware.spark3d.geometryObjects.Point3D
// import com.astrolabsoftware.spark3d.spatial3DRDD._
// import com.astrolabsoftware.spark3d.utils.GridType
//
// // Spark lib
// import org.apache.spark.storage.StorageLevel
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.functions._
//
// // Logger
// import org.apache.log4j.Level
// import org.apache.log4j.Logger
//
// /**
//   * Main app.
//   */
// object Partitioning {
//   // Set to Level.WARN is you want verbosity
//   Logger.getLogger("org").setLevel(Level.WARN)
//   Logger.getLogger("akka").setLevel(Level.WARN)
//
//   // Initialise our spark connector
//   val spark = SparkSession
//     .builder()
//     .appName("partitioning")
//     .getOrCreate()
//
//   /**
//     * Main
//     */
//   def main(args : Array[String]) = {
//
//     // Data file
//     val fn_fits = args(0).toString
//
//     // HDU
//     val hdu = args(1).toString
//
//     // Column names
//     val columns = args(2).toString
//
//     // isSpherical
//     val isSpherical = args(3).toBoolean
//
//     // partitioning
//     val mode = args(4).toString
//
//     // Load the data
//     val options = Map("hdu" -> hdu)
//     val pRDD = new Point3DRDD(
//       spark, fn_fits, columns, isSpherical, "fits", options, StorageLevel.MEMORY_ONLY
//     )
//
//     // Partition it
//     val rdd = mode match {
//         case "nopart" => pRDD.rawRDD
//         case "octree" => pRDD.spatialPartitioning(GridType.OCTREE).cache()
//         case "onion" => pRDD.spatialPartitioning(GridType.LINEARONIONGRID).cache()
//         case _ => throw new AssertionError("Choose between nopart, onion, or octree for the partitioning.")
//     }
//
//     // MC it to minimize flukes
//     for (i <- 0 to 2) {
//       val number = rdd.count()
//       println(s"Number of points ($mode) : $number")
//     }
//   }
// }
