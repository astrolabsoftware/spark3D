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

import com.astrolabsoftware.spark3d.Repartitioning.partitionBy
import com.astrolabsoftware.spark3d.Repartitioning.addSPartitioning

// for implicits
import com.astrolabsoftware.spark3d._

class RepartitioningTest extends FunSuite with BeforeAndAfterAll {

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
  val fn_sphere = "src/test/resources/cartesian_spheres.fits"

  // ugly trick - need a val to make imports
  // This is needed to perform mapPartitions on DataFrame with Row type.
  // See https://stackoverflow.com/questions/44094108/not-able-to-import-spark-implicits-in-scalatest
  // val spark2 = spark
  // import spark2.implicits._

  test("Can you repartition a DataFrame (onion)?") {

    val spark2 = spark
    import spark2.implicits._
    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val dfp2 = addSPartitioning(df, options, 10)
    val dfp3 = partitionBy(dfp2, "partition_id")
    val p = dfp3.mapPartitions(part => Iterator(part.size)).take(1).toList(0)

    assert(dfp3.rdd.getNumPartitions == 10)
    assert(p == 2104)
  }

  test("Can you repartition a DataFrame (octree)?") {

    val spark2 = spark
    import spark2.implicits._
    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "octree")

    val dfp2 = addSPartitioning(df, options, 10)
    val dfp3 = partitionBy(dfp2, "partition_id")

    assert(dfp3.rdd.getNumPartitions == 8)
    assert(dfp3.mapPartitions(part => Iterator(part.size)).take(1).toList(0) == 2282)
  }

  test("Can you repartition a DataFrame based on a user defined column?") {

    val spark2 = spark
    import spark2.implicits._

    // partition ID are 0 or 1
    val df = scala.util.Random.shuffle(0.to(9)).toDF("val")
      .select((col("val") % 2).as("val"))

    val dfp = df.partitionBy("val")

    assert(dfp.rdd.getNumPartitions == 2)
    assert(dfp.mapPartitions(part => Iterator(part.size)).take(1).toList(0) == 5)
  }

  test("Can you repartition a DataFrame using implicits?") {

    val spark2 = spark
    import spark2.implicits._
    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val dfp3 = df.addSPartitioning(options, 10).partitionBy("partition_id")

    assert(dfp3.rdd.getNumPartitions == 10)
    assert(dfp3.mapPartitions(part => Iterator(part.size)).take(1).toList(0) == 2104)
  }

  test("Can you catch an error in the number of partitions when creating the partitioning?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val exception = intercept[AssertionError] {
      df.addSPartitioning(options, -4)
    }
    assert(exception.getMessage.contains("The number of partitions must be strictly greater than zero!"))
  }

  test("Can you catch an error in the number of partitions when repartitioning?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val exception = intercept[AssertionError] {
      df.addSPartitioning(options, 10).partitionBy("partition_id", -4)
    }
    assert(exception.getMessage.contains("The number of partitions must be strictly greater than zero!"))
  }

  test("Can you catch an error in the coordinate system?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "ecliptic",
      "gridtype" -> "onion")

    val exception = intercept[AssertionError] {
      df.addSPartitioning(options, 10)
    }
    assert(exception.getMessage.contains("Coordinate system not understood!"))
  }

  test("Can you catch an error in the geometry of objects?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "triangles",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val exception = intercept[AssertionError] {
      df.addSPartitioning(options, 10)
    }
    assert(exception.getMessage.contains("Geometry not understood!"))
  }

  test("Can you catch an error in the gridtype?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "quadtree")

    val exception = intercept[AssertionError] {
      df.addSPartitioning(options, 10)
    }
    assert(exception.getMessage.contains("Gridtype not understood!"))
  }

  test("Can you return current repartitioning?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "current")

    val dfp = df.addSPartitioning(options)

    assert(dfp.select("partition_id").distinct().collect().size == 1)
  }

  // test("Can you repartition a DataFrame containing cartesian spheres?") {
  //
  //   val df = spark.read.format("fits")
  //     .option("hdu", 1)
  //     .load(fn_sphere)
  //
  //   val options = Map(
  //     "geometry" -> "spheres",
  //     "colnames" -> "x,y,z,radius",
  //     "coordSys" -> "cartesian",
  //     "gridtype" -> "onion")
  //
  //   val dfp3 = df.addSPartitioning(options, 10).partitionBy("partition_id")
  //
  //   assert(dfp3.rdd.getNumPartitions == 10)
  //   assert(dfp3.mapPartitions(part => Iterator(part.size)).take(1) == 2000)
  // }
}
