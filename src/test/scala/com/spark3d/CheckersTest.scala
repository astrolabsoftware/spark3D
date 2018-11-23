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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

// for spark3D implicits
import com.astrolabsoftware.spark3d._

class CheckersTest extends FunSuite with BeforeAndAfterAll {

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

  test("Can you compute the load balancing of a DataFrame (frac)?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val df_repart = df.addSPartitioning(options, 10).repartitionByCol("partition_id", preLabeled = true)
    val df_load = df_repart.checkLoadBalancing()

    assert(df_load.rdd.getNumPartitions == 10)
    assert(df_load.collect().toList.map(_.getDouble(0)).sum.toInt == 100)
  }

  test("Can you compute the load balancing of a DataFrame (size)?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val df_repart = df.addSPartitioning(options, 10).repartitionByCol("partition_id", preLabeled = true)
    val df_load = df_repart.checkLoadBalancing(kind = "size")
    val count = df.count()

    assert(df_load.rdd.getNumPartitions == 10)
    assert(df_load.collect().toList.map(_.getDouble(0)).sum.toInt == count.toInt)
  }

  test("Can you compute the load balancing of a DataFrame (w/ numberOfElements)?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val options = Map(
      "geometry" -> "points",
      "colnames" -> "Z_COSMO,RA,DEC",
      "coordSys" -> "spherical",
      "gridtype" -> "onion")

    val df_repart = df.addSPartitioning(options, 10).repartitionByCol("partition_id", preLabeled = true)
    val df_load = df_repart.checkLoadBalancing(numberOfElements = df_repart.count())

    assert(df_load.rdd.getNumPartitions == 10)
    assert(df_load.collect().toList.map(_.getDouble(0)).sum.toInt == 100)
  }

  test("Can you catch an error in the load balancing check?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val exception = intercept[AssertionError] {
      df.checkLoadBalancing(numberOfElements = -10)
    }
    assert(exception.getMessage.contains("Total number of elements in the DataFrame must be Long greater than 0!"))
  }

  test("Can you catch an error in the kind of load balancing?") {

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fn_point)

    val exception = intercept[AssertionError] {
      df.checkLoadBalancing(kind = "toto")
    }
    assert(exception.getMessage.contains("Wrong value for `kind`!"))
  }
}
