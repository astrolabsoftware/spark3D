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
package com.astrolabsoftware.spark3d

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.{SQLContext, SQLImplicits}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

import com.astrolabsoftware.spark3d.spatialPartitioning.SpatialPartitioner
import com.astrolabsoftware.spark3d.Partitioners

// for implicits
import com.astrolabsoftware.spark3d._

class PartitionersTest extends FunSuite with BeforeAndAfterAll {

  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private val master = "local[2]"
  private val appName = "spark3dtest"

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

  // Test files
  val fn_point = "src/test/resources/astro_obs.csv"

  test("Can you catch a wrong coordinate system?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "elliptical",
      "gridtype" -> "onion")

      val exception = intercept[AssertionError] {
        val P = new Partitioners(df, options)
      }
      assert(exception.getMessage.contains("Coordinate system not understood!"))
  }

  test("Can you catch an unimplemented gridtype?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "quadtree")

      val P = new Partitioners(df, options)

      val exception = intercept[AssertionError] {
        val partitioner = P.get()
      }
      assert(exception.getMessage.contains("Unknown grid type! See utils.GridType for available grids."))
  }

  test("Can you catch a wrong partition number?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "octree")

      val P = new Partitioners(df, options)

      val exception = intercept[AssertionError] {
        val partitioner = P.get(-4)
      }
      assert(exception.getMessage.contains("The number of partitions must be strictly greater than zero!"))
  }

  test("Can you build a partitioner?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "octree")

      val P = new Partitioners(df, options)
      val partitioner = P.get()

      assert(partitioner.isInstanceOf[SpatialPartitioner])
      assert(partitioner.numPartitions == df.rdd.getNumPartitions)
  }

  test("Can you build a partitioner and force the number of partitions?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

      val P = new Partitioners(df, options)
      val partitioner = P.get(10)

      assert(partitioner.isInstanceOf[SpatialPartitioner])
      assert(partitioner.numPartitions == 10)
  }
}
