/*
 * Copyright 2019 Julien Peloton
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
import com.astrolabsoftware.spark3d.utils.Utils._
import com.astrolabsoftware.spark3d.geometryObjects.Point3D

// Spark lib
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Logger
import org.apache.log4j.Level
import org.apache.log4j.Logger

import co.theasi.plotly._

object VisualizePart {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // Initialise our spark connector
  val spark = SparkSession
    .builder()
    .appName("visualisation")
    .getOrCreate()

  import spark.implicits._

  /**
    * Main
    */
  def main(args : Array[String]) = {

    if (args.size < 8) {
      println("""
        Repartition and visualise 3D dataset using plotly.
        The final drawing is always in cartesian coordinates.

        Usage:
        --class com.astrolabsoftware.spark3d.examples.VisualizePart \
        <inputfile> <colnames> <coordSys> <partitionMethod> \
        <npartitions> <fractoplot> <plotly_username> <plotly_api_key>
      """)
    }

    // Data file
    val inputFile = args(0).toString

    // Column names
    val columns = args(1).toString

    // coordSys
    val coordSys : String = args(2).toBoolean match {
        case true => "spherical"
        case false => "cartesian"
    }

    // partitioning
    val grid = args(3).toString

    // partitions
    val part = args(4).toInt

    // partitions
    val fracToPlot = args(5).toDouble

    implicit val server = new writer.Server {
      val credentials = writer.Credentials(args(6).toString, args(7).toString)
      val url = "https://api.plot.ly/v2/"
    }

    // Load the data
    val df = if (inputFile.endsWith("fits")) {
      spark.read.format("fits").option("hdu", 1).load(inputFile)
    } else {
      spark.read.format("parquet").load(inputFile)
    }
    val options = Map(
        "geometry" -> "points",
        "colnames" -> columns,
        "coordSys" -> coordSys,
        "gridtype" -> grid)

    val df_colid = df.prePartition(options, part)
    val df_repart = df_colid.repartitionByCol("partition_id", true, part)

    val splitColnames = columns.split(",")

    val partitionData = df_repart.sample(false, fracToPlot)
     .select(splitColnames(0), splitColnames(1), splitColnames(2))
     .rdd.glom()
     .collect()

    var plot = ThreeDPlot()

    for (partIndex <- 0 to partitionData.size - 1) {
      // Change coordinate system to cartesian
      val partitionDataCart = if (coordSys == "cartesian") {
        partitionData(partIndex)
          .map(x => List(x.getDouble(0), x.getDouble(1), x.getDouble(2))).toList
      } else {
        partitionData(partIndex)
          .map(x => List(x.getFloat(0), dec2theta(x.getFloat(2)), ra2phi(x.getFloat(1))))
          .map(x => new Point3D(x(0), x(1), x(2), true))
          .map(p => sphericalToCartesian(p).getCoordinate).toList
      }

      val meanxt = partitionDataCart.map(x => x.asInstanceOf[List[Double]](0)).toList
      val meanyt = partitionDataCart.map(x => x.asInstanceOf[List[Double]](1)).toList
      val meanzt = partitionDataCart.map(x => x.asInstanceOf[List[Double]](2)).toList

      val markers = if (coordSys == "cartesian") {
        List(ScatterMode.Marker, ScatterMode.Line)
      } else List(ScatterMode.Marker)

      plot = plot.withScatter(meanxt, meanyt, meanzt, ScatterOptions()
        .mode(markers)
        .name(s"Partition $partIndex")
        .marker(MarkerOptions().size(2)))
    }

    // Remove /'s in filename
    val fName = inputFile.split("/").reverse.head
    draw(plot, s"partitioning-$fName-$grid")

  }
}
