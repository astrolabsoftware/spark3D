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
    matchedPartitions ++= octree.getMatchedLeafBoxes(spatialObject.getEnvelope)
    for(partition <- matchedPartitions) {
      result += new Tuple2(partition.indexID, spatialObject)
    }
    result.toIterator
  }

  /**
    * Gets the partitions which contain the input object.
    *
    * @param spatialObject input object for which the containment is to be found
    * @return list of Tuple of containing partitions and their index/partition ID's
    */
  override def getPartitionNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = {

    var partitionNodes = new ListBuffer[Shape3D]
    partitionNodes ++= octree.getMatchedLeafBoxes(spatialObject.getEnvelope)
    var partitionNodesIDs = partitionNodes.map(x => new Tuple2(x.getEnvelope.indexID, x))
    partitionNodesIDs.toList
  }

  /**
    * Gets the partitions which are the neighbors of the partitions which contain the input object.
    *
    * @param spatialObject input object for which the neighbors are to be found
    * @return list of Tuple of neighbor partitions and their index/partition ID's
    */
  override def getNeighborNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = {
    val neighborNodes  = new ListBuffer[Tuple2[Int, Shape3D]]
    val partitionNodes = octree.getMatchedLeaves(spatialObject.getEnvelope)
    for (partitionNode <- partitionNodes) {
      neighborNodes ++= partitionNode.getLeafNeighbors(partitionNode.box.getEnvelope)
    }
    neighborNodes.toList
  }

  /**
    * Gets the partitions which are the neighbors to the input partition. Useful when getting
    * secondary neighbors (neighbors to neighbor) of the queryObject.
    *
    * @param containingNode The boundary of the Node for which neighbors are to be found.
    * @param containingNodeID The index/partition ID of the containingNode
    * @return list of Tuple of secondary neighbor partitions and their index/partition IDs
    */
  override def getSecondaryNeighborNodes[T <: Shape3D](containingNode: T, containingNodeID: Int): List[Tuple2[Int, Shape3D]] = {
    val secondaryNeighborNodes = new ListBuffer[Tuple2[Int, Shape3D]]
    // get the bounding box
    val box = containingNode.getEnvelope
    // reduce the bounding box slightly to avoid getting all the neighbor nodes as the containing nodes
    val searchBox = BoxEnvelope.apply(box.minX+0.0001, box.maxX-0.0001,
        box.minY+0.0001, box.maxY-0.0001,
        box.minZ+0.0001, box.maxZ-0.0001)
    val partitionNodes = octree.getMatchedLeaves(searchBox.getEnvelope)
    // ideally partitionNodes should be of size 1 as the input containingNode is nothing but the
    // boundary of a node in the tree.
    for (partitionNode <- partitionNodes) {
      secondaryNeighborNodes ++= partitionNode.getLeafNeighbors(partitionNode.box.getEnvelope)
    }
    secondaryNeighborNodes.toList
  }

}
