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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD

/**
  * Put here routine to load data for a specific data format
  * Currently available: CSV, JSON, TXT, FITS
  */
object Loader {

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
  def Point3DRDDFromV2(
      spark : SparkSession, filename : String,
      colnames : String, isSpherical: Boolean, format: String,
      options: Map[String, String] = Map("" -> "")): RDD[Point3D] = {

    // Generic load for v2 datasource
    val df = spark.read.format(format).options(options).load(filename)

    // Grab the name of columns
    val csplit = colnames.split(",")

    // Select the 3 columns (x, y, z)
    // and cast to double in case.
    val rawRDD = df.select(
      col(csplit(0)).cast("double"),
      col(csplit(1)).cast("double"),
      col(csplit(2)).cast("double")
    )
    // DF to RDD
    .rdd
    // map to Point3D
    .map(x => new Point3D(
      x.getDouble(0), x.getDouble(1), x.getDouble(2), isSpherical)
    )

    rawRDD
  }

  /**
    * Construct a RDD[ShellEnvelope] from CSV, JSON or TXT data.
    * {{{
    *   // Here is an example with a CSV file containing
    *   // 3 cartesian coordinates + 1 radius columns labeled x,y,z,radius.
    *
    *   // Filename
    *   val fn = "path/to/file.csv"
    *   // Spark datasource
    *   val format = "csv"
    *   // Options to pass to the DataFrameReader - optional
    *   val options = Map("header" -> "true")
    *
    *   // Load the data as RDD[ShellEnvelope]
    *   val rdd = new SphereRDD(spark, fn, "x,y,z,radius", true, format, options)
    * }}}
    *
    * @param spark : (SparkSession)
    *   The spark session
    * @param filename : (String)
    *   File name where the data is stored. Extension must be explicitly
    *   written (.cvs, .json, or .txt)
    * @param colnames : (String)
    *   Comma-separated names of (x, y, z, r) columns to read.
    *   Example: "Z_COSMO,RA,Dec,Radius".
    * @param isSpherical : (Boolean)
    *   If true, it assumes that the coordinates of the center of
    *   the ShellEnvelope are (r, theta, phi).
    *   Otherwise, it assumes cartesian coordinates (x, y, z). Default is false.
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
    * @return (RDD[ShellEnvelope])
    *
    */
  def SphereRDDFromV2(
      spark : SparkSession, filename : String,
      colnames : String, isSpherical: Boolean, format: String,
      options: Map[String, String] = Map("" -> "")): RDD[ShellEnvelope] = {

    // Generic load for v2 datasource
    val df = spark.read.format(format).options(options).load(filename)

    // Grab the name of columns
    val csplit = colnames.split(",")

    // Select the 3 columns (x, y, z) + radius
    // and cast to double in case.
    val rawRDD = df.select(
      col(csplit(0)).cast("double"),
      col(csplit(1)).cast("double"),
      col(csplit(2)).cast("double"),
      col(csplit(3)).cast("double")
    )
      // DF to RDD
      .rdd
      // map to ShellEnvelope
      .map(x => new ShellEnvelope(
      x.getDouble(0), x.getDouble(1), x.getDouble(2), isSpherical, x.getDouble(3))
    )

    rawRDD
  }
}
