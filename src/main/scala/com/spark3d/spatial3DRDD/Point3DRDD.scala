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
package com.astrolabsoftware.spark3d.spatial3DRDD

import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.spatial3DRDD.Loader._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD


class Point3DRDD(rdd : RDD[Point3D], override val isSpherical: Boolean) extends Shape3DRDD[Point3D] {

  /**
    * Construct a RDD[Point3D] from whatever data source registered in Spark.
    * For more information about available official connectors:
    * `https://spark-packages.org/?q=tags%3A%22Data%20Sources%22`
    *
    * This includes: CSV, JSON, TXT, FITS, ROOT, HDF5, ...
    *
    * {{{
    *   // Here is an example with a CSV file containing
    *   // 3 spherical coordinates columns labeled Z_COSMO,RA,Dec.
    *
    *   // Filename
    *   val fn = "path/to/file.csv"
    *   // Spark datasource
    *   val format = "csv"
    *   // Options to pass to the DataFrameReader - optional
    *   val options = Map("header" -> "true")
    *
    *   // Load the data as RDD[Point3D]
    *   val rdd = new Point3DRDD(spark, fn, "Z_COSMO,RA,Dec", true, format, options)
    * }}}
    *
    * @param spark : (SparkSession)
    *   The spark session
    * @param filename : (String)
    *   File name where the data is stored.
    * @param colnames : (String)
    *   Comma-separated names of (x, y, z) columns. Example: "Z_COSMO,RA,Dec".
    * @param isSpherical : (Boolean)
    *   If true, it assumes that the coordinates of the Point3D are (r, theta, phi).
    *   Otherwise, it assumes cartesian coordinates (x, y, z).
    * @param format : (String)
    *   The name of the data source as registered in Spark. For example:
    *     - text
    *     - csv
    *     - json
    *     - com.astrolabsoftware.sparkfits
    *     - org.dianahep.sparkroot
    *     - gov.llnl.spark.hdf or hdf5
    * @param options : (Map[String, String])
    *   Options to pass to the DataFrameReader. Default is no options.
    * @return (RDD[Point3D])
    *
    *
    */
  def this(spark : SparkSession, filename : String, colnames : String, isSpherical: Boolean,
      format: String, options: Map[String, String] = Map("" -> "")) {
    this(Point3DRDDFromV2(spark, filename, colnames, isSpherical, format, options), isSpherical)
  }

  // Raw partitioned RDD
  override val rawRDD = rdd
}

/**
  * Handle point3DRDD.
  */
object Point3DRDD {
  def apply(rdd : RDD[Point3D], isSpherical: Boolean): Point3DRDD = {
    new Point3DRDD(rdd, isSpherical)
  }
}
