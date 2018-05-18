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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.spark3d.geometryObjects.Point3D

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Test class for the Point3DRDD class.
  */
class Point3DRDDTest extends FunSuite with BeforeAndAfterAll {

  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private val master = "local[2]"
  private val appName = "sparkfitsTest"

  private var spark : SparkSession = _

  override protected def beforeAll() : Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }
  // END TODO

  val fn_fits = "src/test/resources/astro_obs.fits"
  test("Can you repartition a RDD with the onion space?") {
    // Load data from a FITS file
    val df = spark.read
      .format("com.sparkfits")
      .option("hdu", 1)
      .load(fn_fits)

    // Reshape the data (x, y, z) into Point3D (euclidean)
    val rdd = df.rdd.map(x => new Point3D(
      x.getFloat(0).toDouble,
      x.getFloat(1).toDouble,
      x.getFloat(2).toDouble)
    )

    // Initialise my 3D RDD
    val pointRDD = new Point3DRDD(rdd)

    // Partition my space
    // TODO: include that in method to update the RDD...
    val pointRDD_part = pointRDD.spatialPartitioning(0.0, 1.0, 0.1)

    // Collect the size of each partition
    val partitions = pointRDD_part.rdd
      .mapPartitions(iter => Array(iter.size).iterator, true).collect()

    assert(partitions.size == 11 && partitions(5) == 979)
  }
}
