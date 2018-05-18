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

import com.spark3d.spatialPartitioning.OnionPartitioning
import com.spark3d.spatialPartitioning.OnionPartitioner
import com.spark3d.geometryObjects._
import com.spark3d.utils.GridType

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.PairFlatMapFunction

/**
  * Class to handle Point3D RDD.
  * It takes as input a RDD[Point3D] with random partitioning, and apply a
  */
class Point3DRDD(rdd: RDD[Point3D]) extends Shape3DRDD[Point3D] {

  def spatialPartitioning(minZ : Double, maxZ : Double, dZ : Double) : JavaRDD[Point3D] = {
    // Initialise our space
    val partitioning = new OnionPartitioning
    partitioning.LinearOnionPartitioning(minZ, maxZ, dZ)

    // Grab the grid elements
    val grids = partitioning.getGrids

    // Build our partitioner
    val partitioner = new OnionPartitioner(GridType.LINEARONIONGRID, grids)

    partition[Point3D](rdd, partitioner)
  }
}
