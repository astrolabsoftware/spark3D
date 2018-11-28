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
import com.astrolabsoftware.spark3d.Queries.windowQuery

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
  val fn_sphe_manual_knn = "src/test/resources/cartesian_spheres_manual_knn.csv"
  val fn_sphe_manual = "src/test/resources/cartesian_spheres_manual.csv"

  test("Can you find the K nearest neighbours (cartesian + no unique)?") {
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

  test("KNN: Can you find the K nearest neighbours (unique vs no unique)?") {
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
    val knnRepartNoUnique = KNN(dfp_repart.select("x", "y", "z"), queryObject, 5000, options("coordSys"), false)
    val knnRepartUnique = KNN(dfp_repart.select("x", "y", "z"), queryObject, 5000, options("coordSys"), true)

    assert(knnRepartNoUnique.distinct.count == 5000)
    assert(knnRepartUnique.distinct.count == 5000)

    // Stupid sum to quickly check they are the same
    assert(knnRepartNoUnique.select("x").collect().map(x => x.getDouble(0)).sum == knnRepartUnique.select("x").collect().map(x => x.getDouble(0)).sum)
  }

  test("KNN: Can you find the K nearest neighbours (spherical)?") {
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

  test("KNN: Do you have the correct behaviour if K=0?") {

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

  test("KNN: Can you catch a wrong DF in input?") {
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

  test("KNN: Can you catch a wrong coordSys?") {
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

  test("KNN: Can you take Float as input?") {
    val spark2 = spark
    import spark2.implicits._

    val df = spark.read.format("fits")
      .option("hdu", 1)
      .load(fn_cart)

    val queryObject = 0.2::0.2::0.2::Nil

    val knn = KNN(df.select(col("x").cast("double"), col("y").cast("double"), col("z").cast("double")), queryObject, 10, "cartesian", false)

    assert(knn.count == 10)
  }

  test("KNN: Can you take Integer as input?") {
    val spark2 = spark
    import spark2.implicits._

    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual_knn)

    val queryObject = List(0.2, 0.2, 0.2)

    val knn = KNN(df.select(col("x").cast("integer"), col("y").cast("integer"), col("z").cast("integer")), queryObject, 2, "cartesian", false)

    assert(knn.count == 2)
  }

  test("WindowQuery: Can you perform a window query (point-like)?") {
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual)

    val windowCoord = List(1.0, 1.0, 1.0)
    val windowType = "point"
    val coordSys = "cartesian"

    val dfEnv = windowQuery(df.select("x", "y", "z"), windowType, windowCoord, coordSys)

    assert(dfEnv.count == 2)
  }

  test("WindowQuery: Can you perform a window query (shell-like)?") {
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual)

    val windowCoord = List(1.0, 1.0, 1.0, 0.0, 2.0)
    val windowType = "shell"
    val coordSys = "cartesian"

    val dfEnv = windowQuery(df.select("x", "y", "z"), windowType, windowCoord, coordSys)

    assert(dfEnv.count == 3)
  }

  test("WindowQuery: Can you perform a window query (sphere-like)?") {
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual)

    val windowCoord = List(1.0, 1.0, 1.0, 2.0)
    val windowType = "sphere"
    val coordSys = "cartesian"

    val dfEnv = windowQuery(df.select("x", "y", "z"), windowType, windowCoord, coordSys)

    assert(dfEnv.count == 3)
  }

  test("WindowQuery: Can you perform a window query (sphere-like + repartitioning)?") {
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "x,y,z",
      "coordSys" -> "cartesian",
      "gridtype" -> "octree")

    val dfp_repart = df.addSPartitioning(options, 10).repartitionByCol("partition_id", preLabeled = true)

    val windowCoord = List(1.0, 1.0, 1.0, 2.0)
    val windowType = "sphere"
    val coordSys = "cartesian"

    val dfEnv = windowQuery(df.select("x", "y", "z"), windowType, windowCoord, coordSys)
    val dfEnv_repart = windowQuery(dfp_repart.select("x", "y", "z"), windowType, windowCoord, coordSys)

    assert(dfEnv_repart.count == 3)
    assert(dfEnv.select("x").collect().map(x => x.getDouble(0)).sum == dfEnv_repart.select("x").collect().map(x => x.getDouble(0)).sum)
  }

  test("WindowQuery: Can you perform a window query (box-like)?") {
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual)

    val windowCoord = List(2.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 2.0)
    val windowType = "box"
    val coordSys = "cartesian"

    val dfEnv = windowQuery(df.select("x", "y", "z"), windowType, windowCoord, coordSys)

    assert(dfEnv.count == 2)
  }

  test("WindowQuery: Can you check wrong windowType for window query?") {
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual)

    val windowCoord = List(2.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 2.0)
    val windowType = "toto"
    val coordSys = "cartesian"

    val exception = intercept[AssertionError] {
      windowQuery(df.select("x", "y", "z"), windowType, windowCoord, coordSys)
    }
    assert(exception.getMessage.contains("windowType not understood!"))
  }

  test("WindowQuery: Can you check input box can take only cartesian coordinates?") {
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual)

    val windowCoord = List(2.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 2.0)
    val windowType = "box"
    val coordSys = "spherical"

    val exception = intercept[AssertionError] {
      windowQuery(df.select("x", "y", "z"), windowType, windowCoord, coordSys)
    }
    assert(exception.getMessage.contains("Input Box coordinates must have cartesian coordinate!"))
  }

  test("WindowQuery: Can you check input DataFrame has 3 columns for window Query?") {
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(fn_sphe_manual)

    val windowCoord = List(2.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 2.0)
    val windowType = "box"
    val coordSys = "cartesian"

    val exception = intercept[AssertionError] {
      windowQuery(df, windowType, windowCoord, coordSys)
    }
    assert(exception.getMessage.contains("Input DataFrame must have 3 columns to perform window query"))
  }
}
