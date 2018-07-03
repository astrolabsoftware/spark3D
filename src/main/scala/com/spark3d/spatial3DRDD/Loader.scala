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
package com.spark3d.spatial3DRDD

import com.spark3d.geometryObjects._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD

/**
  * Put here routine to load data for a specific data format
  * Currently available: CSV, FITS
  */
object Loader {

  /**
    * Construct a RDD[Point3D] from CSV data.
    * {{{
    *   val fn = "src/test/resources/astro_obs.csv"
    *   val rdd = new Point3DRDDFromFITS(spark, fn, "RA,Dec,Z_COSMO", true)
    * }}}
    *
    * @param spark : (SparkSession)
    *   The spark session
    * @param filename : (String)
    *   File name where the data is stored
    * @param colnames : (String)
    * Comma-separated names of (x, y, z) columns. Example: "RA,Dec,Z_COSMO".
    * @param isSpherical : (Boolean)
    *   If true, it assumes that the coordinates of the Point3D are (r, theta, phi).
    *   Otherwise, it assumes cartesian coordinates (x, y, z).
    * @return (RDD[Point3D])
    *
    *
    */
  def Point3DRDDFromCSV(spark : SparkSession, filename : String, colnames : String, isSpherical: Boolean): RDD[Point3D] = {

    val df = spark.read
      .option("header", true)
      .csv(filename)

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
    * Construct a RDD[Point3D] from FITS data.
    * {{{
    *   val fn = "src/test/resources/astro_obs.fits"
    *   val rdd = new Point3DRDDFromFITS(spark, fn, 1, "RA,Dec,Z_COSMO", true)
    * }}}
    *
    * @param spark : (SparkSession)
    *   The spark session
    * @param filename : (String)
    *   File name where the data is stored
    * @param hdu : (Int)
    *   HDU to load.
    * @param colnames : (String)
    * Comma-separated names of (x, y, z) columns. Example: "RA,Dec,Z_COSMO".
    * @param isSpherical : (Boolean)
    *   If true, it assumes that the coordinates of the Point3D are (r, theta, phi).
    *   Otherwise, it assumes cartesian coordinates (x, y, z). Default is false.
    * @return (RDD[Point3D])
    *
    */
  def Point3DRDDFromFITS(spark : SparkSession, filename : String, hdu : Int,
      colnames : String, isSpherical: Boolean): RDD[Point3D] = {

    // Load the data as DataFrame using spark-fits
    val df = spark.read
      .format("com.sparkfits")
      .option("hdu", hdu)
      .load(filename)

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
}
