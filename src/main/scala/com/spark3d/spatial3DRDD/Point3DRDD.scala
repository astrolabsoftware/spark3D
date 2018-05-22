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

import com.spark3d.spatialPartitioning.OnionPartitioning
import com.spark3d.spatialPartitioning.OnionPartitioner
import com.spark3d.geometryObjects._
import com.spark3d.utils.GridType

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.PairFlatMapFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * The Point3DRDD object containing class to handle Point3DRDD classes.
  *
  */
object Point3DRDD {

  /**
    * Class to make a Point3D RDD from FITS data.
    * {{{
    *   val fn = "src/test/resources/astro_obs.fits"
    *   val point3RDD = new Point3DRDD(spark, fn, 1, "RA,Dec,Z_COSMO")
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
    *
    */
  class Point3DRDD(spark : SparkSession, filename : String, hdu : Int, colnames : String) extends Shape3DRDD[Point3D] {

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
        col(csplit(2)).cast("double")
      )
      // DF to RDD
      .rdd
      // map to Point3D
      .map(x => new Point3D(x.getDouble(0), x.getDouble(1), x.getDouble(2)))
  }
}
