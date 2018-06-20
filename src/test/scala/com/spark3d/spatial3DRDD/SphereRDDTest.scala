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
import com.spark3d.geometryObjects.{BoxEnvelope, ShellEnvelope}
import com.spark3d.utils.GridType
import com.spark3d.spatial3DRDD._
import com.spark3d.spatialPartitioning.{Octree, OctreePartitioner, OctreePartitioning, SpatialPartitioner}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

import scala.math.{ceil, floor, log, pow}

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
  val fn_csv = "src/test/resources/cartesian_spheres.csv"
  val fn_csv_manual = "src/test/resources/cartesian_spheres_manual.csv"

  test("FITS: Can you repartition a RDD with the octree space?") {
    val sphereRDD = new SphereRDDFromFITS(spark, fn_fits, 1, "x,y,z,radius", false)

    // Partition the space using the OCTREE
    val sphereRDD_part = sphereRDD.spatialPartitioning(GridType.OCTREE, 100)

    // number of partitions created will be less that or equal to the input number of partitions, and will always be
    // in the powers of 8
    assert(sphereRDD_part.getNumPartitions == 64)
  }

  test("CSV: Can you repartition a RDD with the octree space?") {
    val sphereRDD = new SphereRDDFromCSV(spark, fn_csv_manual,"x,y,z,radius", false)

    // check the data boundary
    val dataBoundary = BoxEnvelope.apply(0.0, 4.0, 0.0, 4.0, 0.0, 4.0)
    assert(sphereRDD.getDataEnvelope.isEqual(dataBoundary))

    // replicating the Shape3DRDD code here to verify the placeObject
    val numPartitionsRaw = 10
    // dataSize will be 16
    val dataSize = sphereRDD.rawRDD.count
    // sampleSize will be 3
    val sampleSize = (dataSize * 0.2).asInstanceOf[Int]
    val samples = sphereRDD.rawRDD.takeSample(false, sampleSize, 12).toList.map(x => x.getEnvelope)
    // maxLevels will be 1, so thereby max 8 nodes
    val maxLevels = floor(log(numPartitionsRaw)/log(8)).asInstanceOf[Int]
    // maxItemsPerBox will be 2
    val maxItemsPerBox = ceil(dataSize  /pow(8, maxLevels)).asInstanceOf[Int]
    // construct Octree with sample data
    val octree = new Octree(sphereRDD.getDataEnvelope, 0, maxItemsPerBox, maxLevels)
    val partitioning = OctreePartitioning.apply(samples, octree)
    val grids = partitioning.getGrids
    val partitioner = new OctreePartitioner(octree, grids)
    var iterator = partitioner.placeObject(new ShellEnvelope(1.0,1.0,1.0,false,0.8))
    var count = 0
    while(iterator.hasNext) {
      count += 1
      iterator.next
    }
    assert(count == 1)
    iterator = partitioner.placeObject(new ShellEnvelope(1.0,1.0,1.0,false,1.0))
    count = 0
    while(iterator.hasNext) {
      count += 1
      iterator.next
    }
    assert(count == 8)


    // Partition the space using the OCTREE
    val sphereRDD_part = sphereRDD.spatialPartitioning(GridType.OCTREE, 10)

    // number of partitions created will be less that or equal to the input number of partitions, and will always be
    // in the powers of 8
    assert(sphereRDD_part.getNumPartitions == 8)
    // Collect the size of each partition
    val partitions = sphereRDD_part.mapPartitions(
      iter => Array(iter.size).iterator, true).collect()

    //data consists of 16 spheres. 15 spheres (there BoxEnvelope) belong to only leaf node and 1
    // sphere belongs to all of the leaf nodes (viz 8)
    assert(partitions.toList.foldLeft(0)(_+_) == 23)
  }

  test("RDD: Can you construct a SphereRDD from a RDD[Shell]?") {
    val pointRDD = new SphereRDDFromCSV(spark, fn_csv, "x,y,z,radius", false)

    val rdd = pointRDD.rawRDD

    val newRDD = new SphereRDDFromRDD(rdd)

    assert(newRDD.isInstanceOf[Shape3DRDD[ShellEnvelope]])
  }
}
