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
import com.astrolabsoftware.spark3d.utils.GridType
import com.astrolabsoftware.spark3d.spatial3DRDD._
import com.astrolabsoftware.spark3d.spatialPartitioning.SpatialPartitioner

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Test class for the Point3DRDD class.
  */
class Point3DRDDTest extends FunSuite with BeforeAndAfterAll {

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

  test("Can you repartition a RDD with the onion space?") {
    val pointRDD = new Point3DRDD(spark, fn_fits, 1, "Z_COSMO,RA,DEC", true)

    // Partition the space using the LINEARONIONGRID
    val pointRDD_part = pointRDD.spatialPartitioning(GridType.LINEARONIONGRID)

    // Collect the size of each partition
    val partitions = pointRDD_part.mapPartitions(
      iter => Array(iter.size).iterator, true).collect()

    assert(partitions.size == 1 && partitions(0) == 20000)
  }

  test("Can you repartition a RDD with the onion space with more partitions?") {
    val pointRDD = new Point3DRDD(spark, fn_fits, 1, "Z_COSMO,RA,DEC", true)

    // Partition my space with 10 data shells using the LINEARONIONGRID
    val pointRDD_part = pointRDD.spatialPartitioning(GridType.LINEARONIONGRID, 10)

    // Collect the size of each partition
    val partitions = pointRDD_part.mapPartitions(
      iter => Array(iter.size).iterator, true).collect()

    assert(partitions.size == 10 && partitions(5) == 2026 && partitions.sum == 20000)
  }

  test("RDD: Can you construct a Point3DRDD from a RDD[Point3D]?") {
    val pointRDD = new Point3DRDD(spark, fn_fits, 1, "Z_COSMO,RA,DEC", true)

    val rdd = pointRDD.rawRDD

    val newRDD = new Point3DRDD(rdd, pointRDD.isSpherical)

    assert(newRDD.isInstanceOf[Shape3DRDD[Point3D]])
  }

  test("Can you repartition a RDD from the partitioner of another?") {
    val pointRDD1 = new Point3DRDD(spark, fn_fits, 1, "Z_COSMO,RA,DEC", true)
    val pointRDD2 = new Point3DRDD(spark, fn_fits, 1, "Z_COSMO,RA,DEC", true)

    // Partition 1st RDD with 10 data shells using the LINEARONIONGRID
    val pointRDD1_part = pointRDD1.spatialPartitioning(GridType.LINEARONIONGRID, 10)
    // Partition 2nd RDD with partitioner of RDD1
    val partitioner = pointRDD1_part.partitioner.get.asInstanceOf[SpatialPartitioner]
    val pointRDD2_part = pointRDD2.spatialPartitioning(partitioner)

    assert(pointRDD1_part.partitioner == pointRDD2_part.partitioner)
  }
}
