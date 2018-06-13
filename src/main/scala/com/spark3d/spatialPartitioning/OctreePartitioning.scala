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

import scala.collection.mutable.ListBuffer

class OctreePartitioning (private val octree: Octree)
  extends Serializable {

  /**
    * @return the octree used for partitioning
    */
  def getPartitionTree(): Octree = {
    octree
  }

  /**
    * @return Leaf nodes of the partitioning tree
    */
  def getGrids(): List[BoxEnvelope] = {
    octree.getLeafNodes.toList
  }
}

object OctreePartitioning {

  def apply(data: List[BoxEnvelope], tree: Octree): OctreePartitioning = {
    for (element <- data) {
      tree.insertElement(element)
    }
    tree.assignPartitionIDs
    new OctreePartitioning(tree)
  }
}
