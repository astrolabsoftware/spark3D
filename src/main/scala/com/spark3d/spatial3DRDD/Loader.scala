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
  * Currently available: CSV, JSON, TXT, FITS
  */
object Loader {

  /**
    * Construct a RDD[Point3D] from CSV, JSON or TXT data.
    * {{{
    *   // CSV
    *   val fn = "src/test/resources/astro_obs.csv"
    *   val rdd = new Point3DRDD(spark, fn, "Z_COSMO,RA,Dec", true)
    *   // JSON
    *   val fn = "src/test/resources/astro_obs.json"
    *   val rdd = new Point3DRDD(spark, fn, "Z_COSMO,RA,Dec", true)
    *   // TXT
    *   val fn = "src/test/resources/astro_obs.txt"
    *   val rdd = new Point3DRDD(spark, fn, "Z_COSMO,RA,Dec", true)
    * }}}
    *
    * @param spark : (SparkSession)
    *   The spark session
    * @param filename : (String)
    *   File name where the data is stored. Extension must be explicitly
    *   written (.cvs, .json, or .txt)
    * @param colnames : (String)
    *   Comma-separated names of (x, y, z) columns. Example: "Z_COSMO,RA,Dec".
    * @param isSpherical : (Boolean)
    *   If true, it assumes that the coordinates of the Point3D are (r, theta, phi).
    *   Otherwise, it assumes cartesian coordinates (x, y, z).
    * @return (RDD[Point3D])
    *
    *
    */
  def Point3DRDDFromText(spark : SparkSession, filename : String, colnames : String, isSpherical: Boolean): RDD[Point3D] = {

    val df = filename match {
      case x if x.contains(".csv") => {
        spark.read
          .option("header", true)
          .csv(filename)
      }
      case x if x.contains(".json") => {
        spark.read
          .option("header", true)
          .json(filename)
      }
      case x if x.contains(".txt") => {
        spark.read
          .option("header", true)
          .option("sep", " ")
          .csv(filename)
      }
      case _ => throw new AssertionError("""
        I do not understand the file format. Accepted extensions are:
        .csv, .json, .txt, or .text
        You can also load FITS file using the HDU option (see Point3DRDDFromFITS)
      """)
    }

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
    *   val rdd = new Point3DRDDFromFITS(spark, fn, 1, "Z_COSMO,RA,Dec", true)
    * }}}
    *
    * @param spark : (SparkSession)
    *   The spark session
    * @param filename : (String)
    *   File name where the data is stored
    * @param hdu : (Int)
    *   HDU to load.
    * @param colnames : (String)
    *   Comma-separated names of (x, y, z) columns. Example: "Z_COSMO,RA,Dec".
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

  /**
    * Construct a RDD[ShellEnvelope] from CSV, JSON or TXT data.
    * {{{
    *   // CSV
    *   val fn = "src/test/resources/cartesian_spheres.csv"
    *   val rdd = new SphereRDD(spark, fn, "x,y,z,radius", false)
    *   // JSON
    *   val fn = "src/test/resources/cartesian_spheres.json"
    *   val rdd = new SphereRDD(spark, fn, "x,y,z,radius", false)
    *   // TXT
    *   val fn = "src/test/resources/cartesian_spheres.txt"
    *   val rdd = new SphereRDD(spark, fn, "x,y,z,radius", false)
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
    * @return (RDD[ShellEnvelope])
    *
    */
  def SphereRDDFromText(
      spark : SparkSession, filename : String, colnames : String,
      isSpherical: Boolean = false): RDD[ShellEnvelope] = {

    val df = filename match {
      case x if x.contains(".csv") => {
        spark.read
          .option("header", true)
          .csv(filename)
      }
      case x if x.contains(".json") => {
        spark.read
          .option("header", true)
          .json(filename)
      }
      case x if x.contains(".txt") => {
        spark.read
          .option("header", true)
          .option("sep", " ")
          .csv(filename)
      }
      case _ => throw new AssertionError("""
        I do not understand the file format. Accepted extensions are:
        .csv, .json, .txt, or .text
        You can also load FITS file using the HDU option (see Point3DRDDFromFITS)
      """)
    }

    // Grab the name of columns
    val csplit = colnames.split(",")

    // Select the 3 columns (x, y, z)
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

  /**
    * Construct a RDD[ShellEnvelope] from FITS data.
    * {{{
    *   val fn = "src/test/resources/cartesian_spheres.fits"
    *   val sphereRDD = new SphereRDD(spark, fn, 1, "x,y,z,radius", false)
    * }}}
    *
    * @param spark : (SparkSession)
    *   The spark session
    * @param filename : (String)
    *   File name where the data is stored
    * @param hdu : (Int)
    *   HDU to load.
    * @param colnames : (String)
    *   Comma-separated names of (x, y, z, r) columns to read.
    *   Example: "Z_COSMO,RA,Dec,Radius".
    * @param isSpherical : (Boolean)
    *   If true, it assumes that the coordinates of the center of
    *   the ShellEnvelope are (r, theta, phi).
    *   Otherwise, it assumes cartesian coordinates (x, y, z). Default is false.
    * @return (RDD[ShellEnvelope)
    *
    */
  def SphereRDDFromFITS(
    spark : SparkSession, filename : String, hdu : Int,
    colnames : String, isSpherical: Boolean = false): RDD[ShellEnvelope] = {

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
