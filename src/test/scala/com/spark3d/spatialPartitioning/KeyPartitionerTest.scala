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
package com.astrolabsoftware.spark3d.spatialPartitioning

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

import com.astrolabsoftware.spark3d.spatialPartitioning.KeyPartitioner

class KeyPartitionerTest extends FunSuite with BeforeAndAfterAll {

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

  val numOfPartitions = 2

  test("Can you build a key Partitioner?") {
    val kp = new KeyPartitioner(numOfPartitions)
    assert(kp.isInstanceOf[KeyPartitioner])
  }

  test("Can you build a key Partitioner (with Int)?") {
    val rdd = spark.sparkContext.parallelize(1::1::0::Nil).map(x => (x, "val"))
    val kp = new KeyPartitioner(numOfPartitions)
    assert(rdd.partitionBy(kp).getNumPartitions == 2)
    assert(kp.getPartition(1).isInstanceOf[Int])
  }

  test("Can you build a key Partitioner (with Long)?") {
    val rdd = spark.sparkContext.parallelize(1L::1L::0L::Nil).map(x => (x, "val"))
    val kp = new KeyPartitioner(numOfPartitions)
    assert(rdd.partitionBy(kp).getNumPartitions == 2)
    assert(kp.getPartition(1L).isInstanceOf[Int])
  }

  test("Can you catch error if the key is not Int or Long?") {
    val kp = new KeyPartitioner(numOfPartitions)
    val exception = intercept[ClassCastException] {
      kp.getPartition("toto")
    }
    assert(exception.getMessage.contains("Key from KeyPartitioner must be Int or Long!"))
  }
}
