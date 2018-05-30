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
package com.spark3d.spatial3DRDD

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.spark3d.utils.GridType
import com.spark3d.spatial3DRDD._
import com.spark3d.spatialPartitioning.SpatialPartitioner
import com.spark3d.spatialOperator._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Test class for the Point3DRDD class.
  */
class CrossMatchTest extends FunSuite with BeforeAndAfterAll {

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
  val fn1 = "src/test/resources/astro_obs.fits"
  val fn2 = "src/test/resources/astro_obs2.fits"

  test("Can you cross match 2 RDDs?") {
    val pointRDD1 = new Point3DRDDFromFITS(spark, fn1, 1, "Z_COSMO,RA,DEC", true)
    val pointRDD2 = new Point3DRDDFromFITS(spark, fn2, 1, "Z_COSMO,RA,DEC", true)

    // Partition 1st RDD with 100 data shells using the LINEARONIONGRID
    val pointRDD1_part = pointRDD1.spatialPartitioning(GridType.LINEARONIONGRID, 100)
    // Partition 2nd RDD with partitioner of RDD1
    val partitioner = pointRDD1_part.partitioner.get.asInstanceOf[SpatialPartitioner]
    val pointRDD2_part = pointRDD2.spatialPartitioning(partitioner)

    val xMatch = CrossMatch.CrossMatchHealpixIndex(pointRDD1_part, pointRDD2_part, 512)

    assert(xMatch.count() == 7175)
  }
}
