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
package com.astrolabsoftware.spark3d.spatial3DRDD

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.spatial3DRDD._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Test class for the 3DRDD classes.
  */
class LoaderTest extends FunSuite with BeforeAndAfterAll {

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
  val fn_fits = "src/test/resources/astro_obs.fits"
  val fn_csv = "src/test/resources/astro_obs.csv"
  val fn_json = "src/test/resources/astro_obs.json"
  val fn_txt = "src/test/resources/astro_obs.txt"
  val fn_wrong = "src/test/resources/astro_obs.wrong"

  val fns_fits = "src/test/resources/cartesian_spheres.fits"
  val fns_csv = "src/test/resources/cartesian_spheres.csv"
  val fns_json = "src/test/resources/cartesian_spheres.json"
  val fns_txt = "src/test/resources/cartesian_spheres.txt"
  val fns_wrong = "src/test/resources/cartesian_spheres.wrong"

  test("FITS: can you read points?") {
    val pointRDD = new Point3DRDD(spark, fn_fits, 1, "Z_COSMO,RA,DEC", true)

    assert(pointRDD.isInstanceOf[Point3DRDD] && pointRDD.rawRDD.count() == 20000)
  }

  test("FITS: can you read spheres?") {
    val sRDD = new SphereRDD(spark, fns_fits, 1, "x,y,z,radius", false)

    assert(sRDD.isInstanceOf[SphereRDD] && sRDD.rawRDD.count() == 20000)
  }

  test("CSV: can you read points?") {
    val pointRDD = new Point3DRDD(spark, fn_csv, "Z_COSMO,RA,DEC", true)

    assert(pointRDD.isInstanceOf[Point3DRDD] && pointRDD.rawRDD.count() == 20000)
  }

  test("CSV: can you read spheres?") {
    val sRDD = new SphereRDD(spark, fns_csv, "x,y,z,radius", false)

    assert(sRDD.isInstanceOf[SphereRDD] && sRDD.rawRDD.count() == 20000)
  }

  test("JSON: can you read points?") {
    val pointRDD = new Point3DRDD(spark, fn_json, "Z_COSMO,RA,DEC", true)

    assert(pointRDD.isInstanceOf[Point3DRDD] && pointRDD.rawRDD.count() == 20000)
  }

  test("JSON: can you read spheres?") {
    val sRDD = new SphereRDD(spark, fns_json, "x,y,z,radius", false)

    assert(sRDD.isInstanceOf[SphereRDD] && sRDD.rawRDD.count() == 20000)
  }

  test("TXT: can you read points?") {
    val pointRDD = new Point3DRDD(spark, fn_txt, "Z_COSMO,RA,DEC", true)

    assert(pointRDD.isInstanceOf[Point3DRDD] && pointRDD.rawRDD.count() == 20000)
  }

  test("TXT: can you read spheres?") {
    val sRDD = new SphereRDD(spark, fns_txt, "x,y,z,radius", false)

    assert(sRDD.isInstanceOf[SphereRDD] && sRDD.rawRDD.count() == 20000)
  }

  test("UNKNOWN: can you catch a file extension error (points)?") {
    val exception = intercept[AssertionError] {
      val pointRDD = new Point3DRDD(spark, fn_wrong, "Z_COSMO,RA,DEC", true)
    }
    assert(exception.getMessage.contains("I do not understand the file format"))
  }

  test("UNKNOWN: can you catch a file extension error (spheres)?") {
    val exception = intercept[AssertionError] {
      val pointRDD = new SphereRDD(spark, fn_wrong, "x,y,z,radius", false)
    }
    assert(exception.getMessage.contains("I do not understand the file format"))
  }
}
