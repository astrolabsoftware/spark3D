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

// spark3d lib
// import com.astrolabsoftware.spark3d.utils.GridType
// import com.astrolabsoftware.spark3d.utils.Utils.sphericalToCartesian
// import com.astrolabsoftware.spark3d.spatial3DRDD.Point3DRDD
import com.astrolabsoftware.spark3d._

// Spark lib
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Logger
import org.apache.log4j.Level
import org.apache.log4j.Logger

// Plotting package
// import smile.plot._
// import java.awt.Color
// import java.awt.{GridLayout, Dimension}
//
// import javax.swing.JFrame
// import javax.swing.JPanel

/**
  * Main app. Load the data of a FITS file, repartition it according to the
  * radial distance in 10 bins, and display the result.
  * The display of the result is done via the SMILE package.
  *
  * Usage mainclass $filename $hdu_index $columns $display
  * See run_scala.sh for more informations.
  */
object OnionSpace {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // Initialise our spark connector
  val spark = SparkSession
    .builder()
    .appName("OnionSpace")
    .getOrCreate()

  import spark.implicits._

  /**
    * Main
    */
  def main(args : Array[String]) = {

    // Display mode
    val display = args(3).toString

    // Data file
    val fn_fits = args(0).toString

    // Load the data as Point3DRDD
    // val options = Map("hdu" -> args(1).toString)
    // val pointRDD = new Point3DRDD(
    //   spark, fn_fits, args(2).toString, true, "fits", options)

    val df = spark.read.format("fits")
      .option("hdu", args(1).toInt)
      .load(fn_fits)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> args(2).toString,
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val dfp = df.addSPartitioning(options, 10).repartitionByCol("partition_id", true)

    // Count the number of partition before, and number of elements per partition
    val partitionsBefore = df.mapPartitions(
      iter => Iterator(iter.size)).collect()

    // Collect the size of each partition after re-partitioning
    val partitionsAfter = dfp.mapPartitions(
      iter => Iterator(iter.size)).take(1).toList(0)

    println(partitionsAfter)
    println(partitionsAfter == 2104)

    println("Before: ", partitionsBefore.toList)
    // println("After : ", partitionsAfter.toList)

    // // Display the result
    // val colors = Array(Color.BLACK, Color.RED, Color.GREEN, Color.BLUE,
    //   Color.PINK, Color.YELLOW, Color.DARK_GRAY, Color.ORANGE,
    //   Color.MAGENTA, Color.CYAN)
    //
    // val data = pointRDD_part.map(
    //   x=> sphericalToCartesian(x).center.getCoordinate.toArray).glom.collect().toArray
    //
    // val window = ScatterPlot.plot(data(0), '.', colors(0))
    //
    // for (part <- 1 to data.size - 2) {
    //   window.points(data(part), '.', colors(part))
    // }
    //
    // display match {
    //   case "show" => {
    //     val frame = new JFrame("Staircase Plot")
    //     frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    //     frame.setLocationRelativeTo(null)
    //     frame.getContentPane().add(window)
    //     frame.setVisible(true)
    //     frame.setSize(new Dimension(500, 500))
    //   }
    //   case "save" => {
    //     val headless = new Headless(window);
    //     headless.pack();
    //     headless.setVisible(true);
    //     headless.setSize(new Dimension(500, 500))
    //     window.save(new java.io.File("myOnionFig.png"))
    //   }
    //   case _ => throw new AssertionError("""
    //     I do not understand the kind of display you want.
    //     Choose between "show" and "save".
    //     """)
    // }
  }
}
