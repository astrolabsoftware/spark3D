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
import com.spark3d.spatial3DRDD.Loader._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD


class SphereRDD(rdd : RDD[ShellEnvelope], override val isSpherical: Boolean) extends Shape3DRDD[ShellEnvelope] {

  /**
    * Construct a RDD[ShellEnvelope] from CSV, JSON or TXT data.
    * {{{
    *   // CSV
    *   val fn = "src/test/resources/myfile.csv"
    *   val rdd = new SphereRDD(spark, fn, "Z_COSMO,RA,Dec,radius", true)
    *   // JSON
    *   val fn = "src/test/resources/myfile.json"
    *   val rdd = new SphereRDD(spark, fn, "Z_COSMO,RA,Dec,radius", true)
    *   // TXT
    *   val fn = "src/test/resources/myfile.txt"
    *   val rdd = new SphereRDD(spark, fn, "Z_COSMO,RA,Dec,radius", true)
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
  def this(spark : SparkSession, filename : String, colnames : String, isSpherical: Boolean) {
    this(SphereRDDFromText(spark, filename, colnames, isSpherical), isSpherical)
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
  def this(spark : SparkSession, filename : String, hdu : Int, colnames : String, isSpherical: Boolean) {
    this(SphereRDDFromFITS(spark, filename, hdu, colnames, isSpherical), isSpherical)
  }

  // Raw partitioned RDD
  override val rawRDD = rdd
}

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
object SphereRDD {
  def apply(rdd : RDD[ShellEnvelope], isSpherical: Boolean): SphereRDD = {
    new SphereRDD(rdd, isSpherical)
  }
}
