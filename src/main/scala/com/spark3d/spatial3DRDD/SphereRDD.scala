/*
 * Copyright 2018 Mayur Bhosale
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
  * Construct a ShellEnvelopeRDD from a RDD[ShellEnvelope]
  *
  * @param rdd : (RDD[ShellEnvelope])
  *   RDD whose elements are ShellEnvelope instances.
  * @param isSpherical : (Boolean)
  *   If true, it assumes that the coordinates of the ShellEnvelope center are (r, theta, phi).
  *   Otherwise, it assumes cartesian coordinates (x, y, z).
  *
  */
class SphereRDDFromRDD(
    rdd : RDD[ShellEnvelope],
    override val isSpherical: Boolean = false) extends Shape3DRDD[ShellEnvelope] {
  override val rawRDD = rdd
}

/**
  * Construct a ShellEnvelopeRDD from CSV data.
  *
  * @param spark : (SparkSession)
  *   The spark session
  * @param filename : (String)
  *   File name where the data is stored
  * @param colnames : (String)
  * Comma-separated names of (x, y, z) columns. Example: "Z_COSMO,RA,Dec,Radius".
  * @param isSpherical : (Boolean)
  *   If true, it assumes that the coordinates of the center of the ShellEnvelope are (r, theta, phi).
  *   Otherwise, it assumes cartesian coordinates (x, y, z).
  *
  *
  */
class SphereRDDFromCSV(
    spark : SparkSession, filename : String, colnames : String,
    override val isSpherical: Boolean = false) extends Shape3DRDD[ShellEnvelope] {

  val df = spark.read
    .option("header", true)
    .csv(filename)

  // Grab the name of columns
  val csplit = colnames.split(",")

  // Select the 3 columns (x, y, z)
  // and cast to double in case.
  override val rawRDD = df.select(
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
}

/**
  * Class to make a ShellEnvelope RDD from FITS data.
  * {{{
  *   val fn = "src/test/resources/cartesian_spheres.fits"
  *   val sphereRDD = new ShellEnvelopeRDD(spark, fn, 1, "Z_COSMO,RA,Dec,Radius")
  * }}}
  *
  * @param spark : (SparkSession)
  *   The spark session
  * @param filename : (String)
  *   File name where the data is stored
  * @param hdu : (Int)
  *   HDU to load.
  * @param colnames : (String)
  * Comma-separated names of (x, y, z, r) columns. Example: "Z_COSMO,RA,Dec,Radius".
  * @param isSpherical : (Boolean)
  *   If true, it assumes that the coordinates of the center of
  *   the ShellEnvelope are (r, theta, phi).
  *   Otherwise, it assumes cartesian coordinates (x, y, z). Default is false.
  *
  */
class SphereRDDFromFITS(
  spark : SparkSession, filename : String, hdu : Int,
  colnames : String, override val isSpherical: Boolean = false) extends Shape3DRDD[ShellEnvelope] {

  // Load the data as DataFrame using spark-fits
  val df = spark.read
    .format("com.sparkfits")
    .option("hdu", hdu)
    .load(filename)

  // Grab the name of columns
  val csplit = colnames.split(",")

  // Select the 3 columns (x, y, z)
  // and cast to double in case.
  override val rawRDD = df.select(
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
}
