/*
 * Copyright 2018 AstroLab Software
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

import com.astrolabsoftware.spark3d.spatialPartitioning._
import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.utils.GridType


class OnionPartitionerTest extends FunSuite with BeforeAndAfterAll {

  test("Can you label correctly a Point3D inside the onion space?") {
    // Initialise our space
    val isSpherical = false
    val partitioning = new OnionPartitioning
    partitioning.LinearOnionPartitioning(10, 1.0, isSpherical)

    // Grab the grid elements
    val grids = partitioning.getGrids

    // Build our partitioner
    val partitioner = new OnionPartitioner(grids)

    // Draw a point and place it on our grid
    val p = new Point3D(0.0, 0.0, 0.45, isSpherical)
    val iterator = partitioner.placeObject(p)

    // Check that the point belongs to the 5th partition (zero based)
    assert(iterator.next()._1 == 4)
  }

  test("Can you label correctly a Point3D outside the onion space?") {
    val isSpherical = false
    val partitioning = new OnionPartitioning
    partitioning.LinearOnionPartitioning(10, 1.0, isSpherical)
    val grids = partitioning.getGrids
    val partitioner = new OnionPartitioner(grids)
    val p = new Point3D(0.0, 0.0, 10.0, isSpherical)
    val iterator = partitioner.placeObject(p)

    // Check that the point lies outside
    // (10 (number of grid elements) + 1 (extra partition) - 1 (index zero based))
    assert(iterator.next()._1 == grids.size - 1)
  }

  test("Can you guess how many partitions are needed for the onion space?") {
    val partitioning = new OnionPartitioning
    partitioning.LinearOnionPartitioning(9, 1.0, false)
    val grids = partitioning.getGrids
    val partitioner = new OnionPartitioner(grids)

    // 9 partitions for inside
    assert(partitioner.numPartitions == 9)
  }

  test("Can you output the hashCode correctly?") {
    val partitioning = new OnionPartitioning
    partitioning.LinearOnionPartitioning(9, 1.0, false)
    val grids = partitioning.getGrids
    val partitioner = new OnionPartitioner(grids)

    assert(partitioner.hashCode == 9)
  }
}
