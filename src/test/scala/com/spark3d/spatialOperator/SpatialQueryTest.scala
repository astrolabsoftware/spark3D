/*
 * Copyright 2018 Mayur Bhosale
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

package com.spark3d.spatialOperator

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.spark3d.spatial3DRDD._
import com.spark3d.geometryObjects.ShellEnvelope
import com.spark3d.utils.GridType
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

class SpatialQueryTest extends FunSuite with BeforeAndAfterAll {

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

  val csv_man = "src/test/resources/cartesian_spheres_manual_knn.csv"

  test("Can you find the K nearest neighbours?") {

    val sphereRDD = new SphereRDDFromCSV(spark, csv_man,"x,y,z,radius", false)
    val sphereRDD_part = sphereRDD.spatialPartitioning(GridType.OCTREE, 10)
    val queryObject =  new ShellEnvelope(1.0,3.0,3.0,false,0.8)

    val knn = SpatialQuery.KNN(queryObject, sphereRDD_part, 3)

    assert(knn.size == 3)

    assert(knn(0).center.isEqual(new ShellEnvelope(2.0,2.0,2.0,false,2.0).center))
    assert(knn(1).center.isEqual(new ShellEnvelope(1.0,1.0,3.0,false,0.8).center))
    assert(knn(2).center.isEqual(new ShellEnvelope(1.0,3.0,0.7,false,0.8).center))
  }
}
