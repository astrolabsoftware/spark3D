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
package com.astrolabsoftware.spark3d.python

import scala.reflect.ClassTag

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.spatial3DRDD._
import com.astrolabsoftware.spark3d.python.PythonClassTag.classTagFromObject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

class PythonClassTagTest extends FunSuite with BeforeAndAfterAll {

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
  val fns_fits = "src/test/resources/cartesian_spheres.fits"

  /**
    * Note that it returns the Java type, and not the Scala type.
    */
  test("Can you retrieve the ClassTag of standard objects?") {

    val aInt : Int = 1
    val ctInt = classTagFromObject(aInt)

    assert(ctInt.toString() == "java.lang.Integer")

  }

  test("Can you retrieve the ClassTag of spark3D objects?") {

    val pt = new Point3D(0.0, 0.0, 0.0, true)
    val ctPoint3D = classTagFromObject(pt)

    assert(ctPoint3D.toString() == "com.astrolabsoftware.spark3d.geometryObjects.Point3D")

  }

  test("Can you retrieve the ClassTag of RDD elements?") {

    val options = Map("hdu" -> "1")
    val sRDD = new SphereRDD(spark, fns_fits, "x,y,z,radius", false, "fits", options)
    val sphere = sRDD.rawRDD.first()
    val ctSphere = classTagFromObject(sphere)

    assert(ctSphere.toString() == "com.astrolabsoftware.spark3d.geometryObjects.ShellEnvelope")

  }
}
