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

// Scala deps
import scala.util.control.Breaks._
import scala.collection.mutable.{HashSet, ListBuffer}

// spark3d deps
import com.astrolabsoftware.spark3d.geometryObjects.ShellEnvelope
import com.astrolabsoftware.spark3d.spatialPartitioning
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._
import com.astrolabsoftware.spark3d.geometryObjects.Point3D


/**
  * Class extending SpatialPartitioner to deal with the onion space.
  * The idea is described here:
  * https://github.com/JulienPeloton/spark3D/issues/11
  *
  * The difference between 2 concentric spheres (shells) will define the
  * elements of the grid (Spark partitions) such that we will have a onion space!
  *
  * @param grids : (List[ShellEnvelope])
  *   List shells which partition the space. Radii of the
  *   shells must be increasing.
  *
  */
class OnionPartitioner(grids : List[ShellEnvelope]) extends SpatialPartitioner(grids) {

  /**
    * The number of partitions is the number of shells defined as the
    * difference between 2 concentric spheres. The n partitions
    * will contain the objects lying in our Onion space.
    *
    * @return (Int) the number of partitions
    */
  override def numPartitions : Int = {
    grids.size
  }

  /**
    * Hashcode returns the number of partitions.
    *
    * @return (Int) the number of partitions
    */
  override def hashCode: Int = numPartitions

  /**
    * Associate geometrical objects (Point3D, Sphere, etc) to
    * grid elements (partition) of the onion space. The association is done
    * according to the position of the center of the object (we do not deal
    * properly with extended objects yet).
    * TODO: Implement a different condition for extended objects?
    *
    * @param spatialObject : (T<:Shape3D)
    *   Shape3D instance (or any extension) representing objects to put on
    *   the grid.
    * @return (Iterator[Tuple2[Int, T]]) Iterable over a Tuple
    *   of (Int, T) where Int is the partition index, and T the input object.
    *
    */
  override def placeObject[T<:Shape3D](spatialObject : T) : Iterator[Tuple2[Int, T]] = {

    // Grab the center of the geometrical objects
    val center = spatialObject.center
    var containFlag : Boolean = false
    val notIncludedID = grids.size - 1
    val result = HashSet.empty[Tuple2[Int, T]]


    // Associate the object with one shell
    breakable {
      for (pos <- 0 to grids.size - 1) {
        val shell = grids(pos)

        if (shell.isPointInShell(center)) {
          result += new Tuple2(pos, spatialObject)
          containFlag = true
          break
        }
      }
    }

    // Useless if Point3D
    if (!containFlag) {
      result += new Tuple2(notIncludedID, spatialObject)
    }

    // Return an iterator
    result.iterator
  }

  /**
    * Associate geometrical objects (Point3D, Sphere, etc) to
    * grid elements (partition) of the onion space. The association is done
    * according to the position of the center of the object (we do not deal
    * properly with extended objects yet).
    * TODO: Implement a different condition for extended objects?
    *
    * @param c0 : Double
    *   First point coordinates
    * @param c1 : Double
    *   Second point coordinates
    * @param c2 : Double
    *   Third point coordinates
    * @param isSpherical : Boolean
    *   true is the coordinate system is spherical, false is cartesian.
    * @return Int: Partition ID in the {{grids}}.
    *
    */
  override def placePoints(c0: Double, c1: Double, c2: Double, isSpherical: Boolean) : Int = {
    val center = new Point3D(c0, c1, c2, isSpherical)
    var containFlag : Boolean = false
    val notIncludedID = grids.size - 1
    val result = HashSet.empty[Int]


    // Associate the object with one shell
    breakable {
      for (pos <- 0 to grids.size - 1) {
        val shell = grids(pos)

        if (shell.isPointInShell(center)) {
          result += pos
          containFlag = true
          break
        }
      }
    }

    // Useless if Point3D
    if (!containFlag) {
      result += notIncludedID
    }

    // Return an iterator
    result.toList(0)
  }

  /**
    * Gets the partitions which contain the input object.
    *
    * @param spatialObject input object for which the containment is to be found
    * @return list of Tuple of containing partitions and their index/partition ID's
    */
  override def getPartitionNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = {
    val center = spatialObject.center
    val partitionNodesIDs = new ListBuffer[Tuple2[Int, Shape3D]]

    breakable {

      for (pos <- 0 to grids.size - 1) {
        val shell = grids(pos)

        if (shell.isPointInShell(center)) {
          partitionNodesIDs += new Tuple2(pos, shell)
          break
        }
      }
    }

    partitionNodesIDs.toList
  }

  /**
    * Gets the partitions which are the neighbors of the partitions which contain the input object.
    *
    * @param spatialObject input object for which the neighbors are to be found
    * @return list of Tuple of neighbor partitions and their index/partition ID's
    */
  override def getNeighborNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = {
    val partitionNodes = getPartitionNodes(spatialObject)
    val neighborNodes = new ListBuffer[Tuple2[Int, Shape3D]]

    if (!partitionNodes.isEmpty) {
      // this implementation assumes that in OnionPartitioning, the object will belong to only one
      // node/partition
      val nodePosition = partitionNodes(0)_1

      // check for corner cases
      if (nodePosition == 0) {
        neighborNodes += new Tuple2(nodePosition + 1, grids(nodePosition + 1))
      } else if (nodePosition == (grids.size - 1)) {
        neighborNodes += new Tuple2(nodePosition - 1, grids(nodePosition - 1))
      } else {
        neighborNodes += new Tuple2(nodePosition + 1, grids(nodePosition + 1))
        neighborNodes += new Tuple2(nodePosition - 1, grids(nodePosition - 1))
      }
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
    // check corner cases
    if (containingNodeID == 0) {
      secondaryNeighborNodes += new Tuple2(containingNodeID + 1, grids(containingNodeID + 1))
    } else if (containingNodeID == (grids.size - 1)) {
      secondaryNeighborNodes += new Tuple2(containingNodeID - 1, grids(containingNodeID - 1))
    } else {
      secondaryNeighborNodes += new Tuple2(containingNodeID + 1, grids(containingNodeID + 1))
      secondaryNeighborNodes += new Tuple2(containingNodeID - 1, grids(containingNodeID - 1))
    }
    secondaryNeighborNodes.toList
  }
}
