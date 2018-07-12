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

package com.astrolabsoftware.spark3d.spatialOperator

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.astrolabsoftware.spark3d.spatial3DRDD._
import com.astrolabsoftware.spark3d.geometryObjects.{Point3D, ShellEnvelope}
import com.astrolabsoftware.spark3d.utils.GridType
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
  val fn_fits = "src/test/resources/cartesian_points.fits"

  test("Can you find the unique K nearest neighbours?") {
    val options = Map("hdu" -> "1")
    val pointRDD = new Point3DRDD(spark, fn_fits, "x,y,z", false, "fits", options)
    val queryObject = new Point3D(0.2, 0.2, 0.2, false)
    // using Octree partitioning
    val pointRDDPart = pointRDD.spatialPartitioning(GridType.OCTREE, 100)
    val knn = SpatialQuery.KNN(queryObject, pointRDDPart, 5000)
    val knnEff = SpatialQuery.KNNEfficient(queryObject, pointRDDPart, 5000)

//    assert(knn.map(x=>x.center.getCoordinate).distinct.size == 5000)
//    assert(knnEff.map(x=>x.center.getCoordinate).distinct.size == 5000)

    // using Onion partitioning
    val pointRDDPart2 = pointRDD.spatialPartitioning(GridType.LINEARONIONGRID, 100)
    val knn2 = SpatialQuery.KNN(queryObject, pointRDDPart2, 5000)
    val knnEff2 = SpatialQuery.KNNEfficient(queryObject, pointRDDPart2, 5000)

//    assert(knn2.map(x=>x.center.getCoordinate).distinct.size == 5000)
//    assert(knnEff2.map(x=>x.center.getCoordinate).distinct.size == 5000)
  }

  test("Can you find the K nearest neighbours correctly?") {

    val options = Map("header" -> "true")
    val sphereRDD = new SphereRDD(spark, csv_man,"x,y,z,radius", false, "csv", options)
    val sphereRDD_part = sphereRDD.spatialPartitioning(GridType.OCTREE, 10)
    val queryObject =  new ShellEnvelope(1.0,3.0,3.0,false,0.8)

    val knn = SpatialQuery.KNN(queryObject, sphereRDD_part, 3)
    val knn2 = SpatialQuery.KNNEfficient(queryObject, sphereRDD_part, 3)
    assert(knn.size == 3)

    assert(knn(0).center.isEqual(new ShellEnvelope(2.0,2.0,2.0,false,2.0).center))
    assert(knn(1).center.isEqual(new ShellEnvelope(1.0,1.0,3.0,false,0.8).center))
    assert(knn(2).center.isEqual(new ShellEnvelope(1.0,3.0,0.7,false,0.8).center))

    assert(knn2(0).center.isEqual(new ShellEnvelope(2.0,2.0,2.0,false,2.0).center))
    assert(knn2(1).center.isEqual(new ShellEnvelope(1.0,1.0,3.0,false,0.8).center))
    assert(knn2(2).center.isEqual(new ShellEnvelope(1.0,3.0,0.7,false,0.8).center))
  }


}
