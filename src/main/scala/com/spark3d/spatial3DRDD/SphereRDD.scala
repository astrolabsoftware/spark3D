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
package com.astrolabsoftware.spark3d.spatial3DRDD

import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.spatial3DRDD.Loader._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD


class SphereRDD(rdd : RDD[ShellEnvelope],
    override val isSpherical: Boolean, storageLevel: StorageLevel) extends Shape3DRDD[ShellEnvelope] {

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
    * @param storageLevel : (StorageLevel)
    *   Storage level for the raw RDD (unpartitioned). Default is StorageLevel.NONE.
    *   See https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence
    *   for more information.
    * @return (RDD[ShellEnvelope])
    *
    */
  def this(spark : SparkSession, filename : String,
      colnames : String, isSpherical: Boolean,
      format: String, options: Map[String, String] = Map("" -> ""),
      storageLevel: StorageLevel = StorageLevel.NONE) {
    this(SphereRDDFromV2(spark, filename, colnames, isSpherical, format, options),
      isSpherical, storageLevel
    )
  }

  // Raw partitioned RDD
  override val rawRDD = rdd
  rawRDD.persist(storageLevel)
}

/**
  * Construct a ShellEnvelopeRDD from a RDD[ShellEnvelope]
  *
  * @param rdd : (RDD[ShellEnvelope])
  *   RDD whose elements are ShellEnvelope instances.
  * @param isSpherical : (Boolean)
  *   If true, it assumes that the coordinates of the ShellEnvelope
  *   center are (r, theta, phi).
  *   Otherwise, it assumes cartesian coordinates (x, y, z).
  * @param storageLevel : (StorageLevel)
  *   Storage level for the raw RDD (unpartitioned). Default is StorageLevel.NONE.
  *   See https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence
  *   for more information.
  *
  */
object SphereRDD {
  def apply(rdd : RDD[ShellEnvelope], isSpherical: Boolean, storageLevel: StorageLevel): SphereRDD = {
    new SphereRDD(rdd, isSpherical, storageLevel)
  }
}
