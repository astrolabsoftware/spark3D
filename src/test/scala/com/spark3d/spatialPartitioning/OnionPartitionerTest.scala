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
package com.spark3d.spatialPartitioning

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.spark3d.spatialPartitioning._
import com.spark3d.geometryObjects._
import com.spark3d.utils.GridType


class OnionPartitionerTest extends FunSuite with BeforeAndAfterAll {

  test("Can you label correctly a Point3D inside the onion space?") {
    // Initialise our space
    val partitioning = new OnionPartitioning
    partitioning.OnionPartitioning(0.0, 1.0, 0.1)

    // Grab the grid elements
    val grids = partitioning.getGrids

    // Build our partitioner
    val partitioner = new OnionPartitioner(GridType.ONIONGRID, grids)

    // Draw a point and place it on our grid
    val p = new Point3D(0.0, 0.0, 0.45)
    val iterator = partitioner.placeObject(p)

    // Check that the point belongs to the 5th partition (zero based)
    assert(iterator.next()._1 == 4)
  }

  test("Can you label correctly a Point3D outside the onion space?") {
    val partitioning = new OnionPartitioning
    partitioning.OnionPartitioning(0.0, 1.0, 0.1)
    val grids = partitioning.getGrids
    val partitioner = new OnionPartitioner(GridType.ONIONGRID, grids)
    val p = new Point3D(0.0, 0.0, 10.0)
    val iterator = partitioner.placeObject(p)

    // Check that the point lies outside (-1 index)
    assert(iterator.next()._1 == -1)
  }

  test("Can you guess how many partitions are needed for the onion space?") {
    val partitioning = new OnionPartitioning
    partitioning.OnionPartitioning(0.0, 0.85, 0.1)
    val grids = partitioning.getGrids
    val partitioner = new OnionPartitioner(GridType.ONIONGRID, grids)

    assert(partitioner.numPartitions == 9)
  }
}
