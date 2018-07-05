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
package com.astrolabsoftware.spark3d.spatialPartitioning

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.astrolabsoftware.spark3d.geometryObjects._

import scala.collection.mutable.ListBuffer

class OctreePartitionerTest extends FunSuite with BeforeAndAfterAll {

    test ("Can you correctly place a Point3D inside the Octree space?") {

    var valid_tree = new Octree(BoxEnvelope.apply(0.0, 4.0, 0.0, 4.0, 0.0, 4.0), 0, null, 2)
    val element1 = BoxEnvelope.apply(0.0, 1.0, 0.0, 1.0, 0.0, 1.0)
    val element2 = BoxEnvelope.apply(1.0, 3.0, 1.0, 3.0, 1.0, 3.0)
    val element3 = BoxEnvelope.apply(1.0, 2.0, 1.0, 2.0, 1.0, 2.0)
    val element4 = BoxEnvelope.apply(0.0, 1.0, 1.0, 2.0, 0.0, 1.0)
    val data = new ListBuffer[BoxEnvelope]
    data += element1
    data += element2
    data += element3
    data += element4

    val partitioning = OctreePartitioning(data.toList, valid_tree)
    val partitioner = new OctreePartitioner(partitioning.getPartitionTree, partitioning.getGrids)
    assert(partitioner.numPartitions == 15)
    var spr = new ShellEnvelope(0.5, 0.5, 0.5, false, 0.2)
    var result = partitioner.placeObject(spr)
    assert(result.next._1 == 13)

    // case when object belongs to all partitions
    spr = new ShellEnvelope(2, 2, 2, false, 1)
    result = partitioner.placeObject(spr)
    var resultCount = 0
    while (result.hasNext) {
      resultCount += 1
      result.next
    }
    assert(resultCount == 15)
  }
}
