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
package com.spark3d.spatial3DRDD

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.spark3d.geometryObjects.Point3D
import com.spark3d.utils.GridType
import com.spark3d.spatial3DRDD._
import com.spark3d.spatialPartitioning.SpatialPartitioner

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

class SphereRDDTest extends FunSuite with BeforeAndAfterAll {

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

  val fn_fits = "src/test/resources/cartesian_spheres.fits"
  val fn_csv = "src/test/resources/cartesian_spheres.fits.csv"

  test("FITS: Can you repartition a RDD with the octree space?") {
    val sphereRDD = new SphereRDDFromFITS(spark, fn_fits, 1, "x,y,z,radius", false)

    // Partition the space using the OCTREE
    val sphereRDD_part = sphereRDD.spatialPartitioning(GridType.LINEARONIONGRID)

    // Collect the size of each partition
    val partitions = pointRDD_part.mapPartitions(
      iter => Array(iter.size).iterator, true).collect()

    assert(partitions.size == 1 && partitions(0) == 20000)
  }

}
