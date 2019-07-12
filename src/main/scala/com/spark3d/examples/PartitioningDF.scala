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
package com.astrolabsoftware.spark3d.examples

// spark3D implicits
import com.astrolabsoftware.spark3d._

// Spark lib
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Logger
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Main app.
  */
object PartitioningDF {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // Initialise our spark connector
  val spark = SparkSession
    .builder()
    .appName("partitioning")
    .getOrCreate()

  import spark.implicits._

  /**
    * Main
    */
  def main(args : Array[String]) = {

    // Data file
    val fn_fits = args(0).toString

    // HDU
    val hdu = args(1).toString

    // Column names
    val columns = args(2).toString

    // isSpherical
    val isSpherical : String = args(3).toBoolean match {
        case true => "spherical"
        case false => "cartesian"
    }

    // partitioning
    val grid = args(4).toString

    // partitions
    val part = args(5).toInt

    // Mode
    val mode = args(6).toString

    // Load the data
    val df = spark.read.format("fits").option("hdu", 1).load(fn_fits)
    val options = Map(
        "geometry" -> "points",
        "colnames" -> columns,
        "coordSys" -> isSpherical,
        "gridtype" -> grid)

    val df_colid = df.prePartition(options, part)

    // MC it to minimize flukes
    for (i <- 0 to 1) {
        var number = mode match {
            case "col" => df_colid.repartitionByCol("partition_id", true, part).mapPartitions(part => Iterator(part.size)).collect().toList
            case "range" => df_colid.repartitionByRange(part, col("partition_id")).mapPartitions(part => Iterator(part.size)).collect().toList
            case "order" => df_colid.orderBy(col("partition_id")).mapPartitions(part => Iterator(part.size)).collect().toList
            case "rep" => df_colid.repartition(part, col("partition_id")).mapPartitions(part => Iterator(part.size)).collect().toList
            case "col-no" => df_colid.repartitionByCol("partition_id", false).mapPartitions(part => Iterator(part.size)).collect().toList
            case _ => throw new NotImplementedError()
        }
        println(s"Number of points ($mode) : $number")
    }
  }
}
