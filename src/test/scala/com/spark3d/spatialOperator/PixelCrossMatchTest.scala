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
package com.spark3d.spatialOperator

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.spark3d.utils.GridType
import com.spark3d.spatial3DRDD._
import com.spark3d.spatialPartitioning.SpatialPartitioner

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Test class for the Point3DRDD class.
  */
class PixelCrossMatchTest extends FunSuite with BeforeAndAfterAll {

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
  val fnA = "src/test/resources/astro_obs_A_light.fits"
  val fnB = "src/test/resources/astro_obs_B_light.fits"

  test("Can you cross match A and B and return A?") {

    val pointRDDA = new Point3DRDDFromFITS(spark, fnA, 1, "Z_COSMO,RA,DEC", true)
    val pointRDDB = new Point3DRDDFromFITS(spark, fnB, 1, "Z_COSMO,RA,DEC", true)

    // Partition 1st RDD with 100 data shells using the LINEARONIONGRID
    val pointRDDA_part = pointRDDA.spatialPartitioning(GridType.LINEARONIONGRID, 100)
    // Partition 2nd RDD with partitioner of RDDA
    val partitioner = pointRDDA_part.partitioner.get.asInstanceOf[SpatialPartitioner]
    val pointRDDB_part = pointRDDB.spatialPartitioning(partitioner)

    val xMatch = PixelCrossMatch.CrossMatchHealpixIndex(pointRDDA_part, pointRDDB_part, 512, "A")

    assert(xMatch.count() == 15)
  }

  test("Can you cross match A and B and return B?") {

    val pointRDDA = new Point3DRDDFromFITS(spark, fnA, 1, "Z_COSMO,RA,DEC", true)
    val pointRDDB = new Point3DRDDFromFITS(spark, fnB, 1, "Z_COSMO,RA,DEC", true)

    // Partition 1st RDD with 100 data shells using the LINEARONIONGRID
    val pointRDDA_part = pointRDDA.spatialPartitioning(GridType.LINEARONIONGRID, 100)
    // Partition 2nd RDD with partitioner of RDDA
    val partitioner = pointRDDA_part.partitioner.get.asInstanceOf[SpatialPartitioner]
    val pointRDDB_part = pointRDDB.spatialPartitioning(partitioner)

    val xMatch = PixelCrossMatch.CrossMatchHealpixIndex(pointRDDA_part, pointRDDB_part, 512, "B")

    assert(xMatch.count() == 15)
  }

  test("Can you cross match A and B and return (A,B)?") {

    val pointRDDA = new Point3DRDDFromFITS(spark, fnA, 1, "Z_COSMO,RA,DEC", true)
    val pointRDDB = new Point3DRDDFromFITS(spark, fnB, 1, "Z_COSMO,RA,DEC", true)

    // Partition 1st RDD with 100 data shells using the LINEARONIONGRID
    val pointRDDA_part = pointRDDA.spatialPartitioning(GridType.LINEARONIONGRID, 100)
    // Partition 2nd RDD with partitioner of RDDA
    val partitioner = pointRDDA_part.partitioner.get.asInstanceOf[SpatialPartitioner]
    val pointRDDB_part = pointRDDB.spatialPartitioning(partitioner)

    val xMatch = PixelCrossMatch.CrossMatchHealpixIndex(pointRDDA_part, pointRDDB_part, 512, "AB")

    assert(xMatch.count() == 15)
  }

  test("Can you cross match A and B and return healpix indices?") {

    val pointRDDA = new Point3DRDDFromFITS(spark, fnA, 1, "Z_COSMO,RA,DEC", true)
    val pointRDDB = new Point3DRDDFromFITS(spark, fnB, 1, "Z_COSMO,RA,DEC", true)

    // Partition 1st RDD with 100 data shells using the LINEARONIONGRID
    val pointRDDA_part = pointRDDA.spatialPartitioning(GridType.LINEARONIONGRID, 100)
    // Partition 2nd RDD with partitioner of RDDA
    val partitioner = pointRDDA_part.partitioner.get.asInstanceOf[SpatialPartitioner]
    val pointRDDB_part = pointRDDB.spatialPartitioning(partitioner)

    val xMatch = PixelCrossMatch.CrossMatchHealpixIndex(pointRDDA_part, pointRDDB_part, 512, "healpix")

    assert(xMatch.count() == 15)
  }

  test("Can you catch an error in pixel cross match (wrong name)?") {

    val pointRDDA = new Point3DRDDFromFITS(spark, fnA, 1, "Z_COSMO,RA,DEC", true)
    val pointRDDB = new Point3DRDDFromFITS(spark, fnB, 1, "Z_COSMO,RA,DEC", true)

    // Partition 1st RDD with 100 data shells using the LINEARONIONGRID
    val pointRDDA_part = pointRDDA.spatialPartitioning(GridType.LINEARONIONGRID, 100)
    // Partition 2nd RDD with partitioner of RDDA
    val partitioner = pointRDDA_part.partitioner.get.asInstanceOf[SpatialPartitioner]
    val pointRDDB_part = pointRDDB.spatialPartitioning(partitioner)

    val exception = intercept[AssertionError] {
      PixelCrossMatch.CrossMatchHealpixIndex(pointRDDA_part, pointRDDB_part, 512, "toto")
    }
    assert(exception.getMessage.contains("I do not know how to perform the cross match."))
  }

  test("Can you catch an error in pixel cross match (different partitioners)?") {

    val pointRDDA = new Point3DRDDFromFITS(spark, fnA, 1, "Z_COSMO,RA,DEC", true)
    val pointRDDB = new Point3DRDDFromFITS(spark, fnB, 1, "Z_COSMO,RA,DEC", true)

    // Partition 1st RDD with 100 data shells using the LINEARONIONGRID
    val pointRDDA_part = pointRDDA.spatialPartitioning(GridType.LINEARONIONGRID, 100)
    // Partition 2nd RDD with 50 data shells using the LINEARONIONGRID
    val pointRDDB_part = pointRDDB.spatialPartitioning(GridType.LINEARONIONGRID, 50)

    val exception = intercept[AssertionError] {
      PixelCrossMatch.CrossMatchHealpixIndex(pointRDDA_part, pointRDDB_part, 512, "B")
    }
    assert(exception.getMessage.contains("The two RDD must be partitioned by the same partitioner"))
  }
}
