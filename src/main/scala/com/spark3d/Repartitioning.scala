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
package com.astrolabsoftware.spark3d

import java.util.HashMap

import org.apache.spark.Partitioner
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession

import com.astrolabsoftware.spark3d.Partitioners

/**
  * Main object containing methods to repartition DataFrames
  *
  * partitionBy(DataFrame, SpatialPartitioner, Map[String, String]): DataFrame
  */
object Repartitioning {

  /**
    * Method to repartition a DataFrame. This method allows to use a custom
    * partitioner (SpatialPartitioner). {{options}} must contain four entries:
    *   - gridtype: the type of repartitioning. Available: onion, octree.
    *   - geometry: geometry of objects: points, spheres, or boxes
    *   - coordSys: coordinate system: spherical or cartesian
    *   - colnames: comma-separated names of the spatial coordinates. For points,
    *   must be "x,y,z" or "r,theta,phi". For spheres, must be "x,y,z,R" or
    *   "r,theta,phi,R".
    *
    * @param df : Input DataFrame
    * @param options : Map[String, String] containing metadata (see above).
    * @return repartitioned DataFrame. Note that an additional column {{partition_id}} is added.
    */
  def partitionBy(df : DataFrame, options: Map[String, String], numPartitions : Int = -1) : DataFrame = {

    // Geometry of objects
    val geometry : String = options("geometry")

    // Definition of the coordinate system. Spherical or cartesian
    val isSpherical : Boolean = options("coordSys") match {
      case "spherical" => true
      case _ => false
    }

    // Column names must be comma-separated.
    val colnames : Array[String] = options("colnames").split(",")

    val P = new Partitioners(df, options)
    val partitioner = P.get(numPartitions)

    // Add a column with the new partition indices
    val dfup = geometry match {
      case "points" => {
        // UDF for the repartitioning
        val placePointsUDF = udf[Int, Double, Double, Double, Boolean](partitioner.placePoints)

        df.withColumn("partition_id",
          placePointsUDF(
            col(colnames(0)).cast("double"),
            col(colnames(1)).cast("double"),
            col(colnames(2)).cast("double"),
            lit(isSpherical)
          )
        )
      }
      case "spheres" => {
        // UDF for the repartitioning
        val placePointsUDF = udf[Int, Double, Double, Double, Double, Boolean](partitioner.placeSpheres)

        df.withColumn("partition_id",
          placePointsUDF(
            col(colnames(0)).cast("double"),
            col(colnames(1)).cast("double"),
            col(colnames(2)).cast("double"),
            col(colnames(3)).cast("double"),
            lit(isSpherical)
          )
        )
      }
      // Need to handle errors!
    }

    // Apply the partitioning in the RDD world
    val rdd = dfup.rdd
      .map(x => (x(x.size - 1), x))
      .partitionBy(partitioner)
      .values

    // Go back to DF
    SparkSession.getActiveSession.get.createDataFrame(rdd, dfup.schema)
  }
}
