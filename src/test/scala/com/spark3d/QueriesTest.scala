/*
 * Copyright 2018 AstroLab Software
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

import com.astrolabsoftware.spark3d._
import com.astrolabsoftware.spark3d.Queries.KNN

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

import org.apache.log4j.Level
import org.apache.log4j.Logger

class QueriesTest extends FunSuite with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private val master = "local[2]"
  private val appName = "spark3dtest"

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
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

  val fn_cart = "src/test/resources/cartesian_spheres.fits"
  val fn_sphe = "src/test/resources/astro_obs.fits"

  test("Can you find the K nearest neighbours (cartesian)?") {
    val spark2 = spark
    import spark2.implicits._

    val df = spark.read.format("fits")
      .option("hdu", 1)
      .load(fn_cart)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "x,y,z",
      "coordSys" -> "cartesian",
      "gridtype" -> "octree")

    val dfp_repart = df.addSPartitioning(options, 10).repartitionByCol("partition_id", preLabeled = true)

    val queryObject = 0.2::0.2::0.2::Nil

    // using Octree partitioning
    val knn = KNN(df.select("x", "y", "z"), queryObject, 5000, options("coordSys"), false)
    val knnRepart = KNN(dfp_repart.select("x", "y", "z"), queryObject, 5000, options("coordSys"), false)
    // val knnEff = SpatialQuery.KNNEfficient(pointRDDPart, queryObject, 5000)

    assert(knn.distinct.count == 5000)
    assert(knnRepart.distinct.count == 5000)

    // Stupid sum to quickly check they are the same
    assert(knn.select("x").collect().map(x => x.getDouble(0)).sum == knnRepart.select("x").collect().map(x => x.getDouble(0)).sum)
    // assert(knnEff.map(x=>x.center.getCoordinate).distinct.size == 5000)
  }

  test("Can you find the K nearest neighbours (spherical)?") {
    val spark2 = spark
    import spark2.implicits._

    val df = spark.read.format("fits")
      .option("hdu", 1)
      .load(fn_sphe)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val dfp_repart = df.addSPartitioning(options, 10).repartitionByCol("partition_id", preLabeled = true)

    val queryObject = 0.0::0.0::0.0::Nil

    // using Octree partitioning
    val knn = KNN(df.select("Z_COSMO", "RA", "DEC"), queryObject, 5000, options("coordSys"), false)
    val knnRepart = KNN(dfp_repart.select("Z_COSMO", "RA", "DEC"), queryObject, 5000, options("coordSys"), false)
    // val knnEff = SpatialQuery.KNNEfficient(pointRDDPart, queryObject, 5000)

    assert(knn.distinct.count == 5000)
    assert(knnRepart.distinct.count == 5000)

    // Stupid sum to quickly check they are the same
    assert(knn.select("Z_COSMO").collect().map(x => x.getDouble(0)).sum == knnRepart.select("Z_COSMO").collect().map(x => x.getDouble(0)).sum)
    // assert(knnEff.map(x=>x.center.getCoordinate).distinct.size == 5000)
  }

  test("Do you have the correct behaviour if K=0?") {

    val spark2 = spark
    import spark2.implicits._

    val df = spark.read.format("fits")
      .option("hdu", 1)
      .load(fn_cart)

    val queryObject = 0.2::0.2::0.2::Nil

    // using Octree partitioning
    val knn = KNN(df.select("x", "y", "z"), queryObject, 0, "cartesian", false)
    assert(knn.count == 0)
  }

  test("Can you catch a wrong DF in input?") {
    val spark2 = spark
    import spark2.implicits._

    val df = spark.read.format("fits")
      .option("hdu", 1)
      .load(fn_cart)

    val queryObject = 0.2::0.2::0.2::Nil

    val exception = intercept[AssertionError] {
      KNN(df, queryObject, 10, "cartesian", false)
    }
    assert(exception.getMessage.contains("Input DataFrame must have 3 columns to perform KNN"))
  }

  test("Can you catch a wrong coordSys?") {
    val spark2 = spark
    import spark2.implicits._

    val df = spark.read.format("fits")
      .option("hdu", 1)
      .load(fn_cart)

    val queryObject = 0.2::0.2::0.2::Nil

    val exception = intercept[AssertionError] {
      KNN(df.select("x", "y", "z"), queryObject, 10, "toto", false)
    }
    assert(exception.getMessage.contains("Coordinate system not understood!"))
  }

  test("Can you take Float as input?") {
    val spark2 = spark
    import spark2.implicits._

    val df = spark.read.format("fits")
      .option("hdu", 1)
      .load(fn_cart)

    val queryObject = 0.2::0.2::0.2::Nil

    val knn = KNN(df.select(col("x").cast("double"), col("y").cast("double"), col("z").cast("double")), queryObject, 10, "cartesian", false)

    assert(knn.count == 10)
  }
}
