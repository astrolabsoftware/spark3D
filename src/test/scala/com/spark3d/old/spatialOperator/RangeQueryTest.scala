// /*
//  * Copyright 2018 Julien Peloton
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// package com.astrolabsoftware.spark3d.spatialOperator
//
// import org.scalatest.{BeforeAndAfterAll, FunSuite}
//
// import com.astrolabsoftware.spark3d.spatial3DRDD._
// import com.astrolabsoftware.spark3d.geometryObjects.ShellEnvelope
// import com.astrolabsoftware.spark3d.geometryObjects.Point3D
//
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.functions._
//
// import org.apache.log4j.Level
// import org.apache.log4j.Logger
//
// /**
//   * Test class for the Point3DRDD class.
//   */
// class RangeQueryTest extends FunSuite with BeforeAndAfterAll {
//
//   // Set to Level.WARN is you want verbosity
//   Logger.getLogger("org").setLevel(Level.OFF)
//   Logger.getLogger("akka").setLevel(Level.OFF)
//
//   private val master = "local[2]"
//   private val appName = "spark3dtest"
//
//   private var spark : SparkSession = _
//
//   override protected def beforeAll() : Unit = {
//     super.beforeAll()
//     spark = SparkSession
//       .builder()
//       .master(master)
//       .appName(appName)
//       .getOrCreate()
//   }
//
//   override protected def afterAll(): Unit = {
//     try {
//       spark.sparkContext.stop()
//     } finally {
//       super.afterAll()
//     }
//   }
//   // END TODO
//
//   // Test files
//   val fn = "src/test/resources/astro_obs_A_light.fits"
//
//   test("Can you find all points within a given region?") {
//
//     val options = Map("hdu" -> "1")
//     val pRDD = new Point3DRDD(spark, fn, "Z_COSMO,RA,DEC", true, "fits", options)
//
//     // Window is a Sphere centered on (0.05, 0.05, 0.05) and radius 0.1.
//     val p = new Point3D(0.05, 0.05, 0.05, true)
//     val window = new ShellEnvelope(p, 0.1)
//
//     val matches = RangeQuery.windowQuery(pRDD.rawRDD, window)
//
//     assert(matches.count() == 182)
//   }
// }
