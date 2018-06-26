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
package com.spark3d.spatialPartitioning

import com.spark3d.geometryObjects.BoxEnvelope
import com.spark3d.geometryObjects.Shape3D.Shape3D

import scala.collection.mutable.{HashSet, ListBuffer}

class OctreePartitioner (octree: Octree, grids : List[BoxEnvelope]) extends SpatialPartitioner(grids) {

  /**
    * Get the number of partitions in this partitioning
    *
    * @return the number of partitions
    */
  override def numPartitions: Int = {
    grids.size
  }

  /**
    * Gets the iterator on tuple leaf nodes (partitions) which intersects, contains or are contained
    * by the input object.
    *
    * @param spatialObject : (T<:Shape3D)
    *   Shape3D instance (or any extension) representing objects to put on
    *   the grid.
    * @return (Iterator[Tuple2[Int, T]) Iterable over a Tuple
    *         *   of (Int, T) where Int is the partition index, and T the input object.
    *
    */
  override def placeObject[T <: Shape3D](spatialObject: T): Iterator[Tuple2[Int, T]] = {

    val result = HashSet.empty[Tuple2[Int, T]]
    var matchedPartitions = new ListBuffer[BoxEnvelope]
    matchedPartitions ++= octree.getMatchedLeaves(spatialObject.getEnvelope)
    for(partition <- matchedPartitions) {
      result += new Tuple2(partition.indexID, spatialObject)
    }
    result.toIterator
  }

  override def getPartitionNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = {

    var partitionNodes = new ListBuffer[Shape3D]
    partitionNodes ++= octree.getMatchedLeaves(spatialObject.getEnvelope)
    var partitionNodesIDs = partitionNodes.map(x => new Tuple2(x.getEnvelope.indexID, x))
    partitionNodesIDs.toList
  }

  override def getNeighborNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = {
    val neighborNodes  = new ListBuffer[Tuple2[Int, Shape3D]]
    val partitionNodes = getPartitionNodes(spatialObject)
    for (partitionNode <- partitionNodes) {
      neighborNodes ++= octree.getLeafNeighbors(partitionNode._2.getEnvelope)
    }
    neighborNodes.toList
  }
}
