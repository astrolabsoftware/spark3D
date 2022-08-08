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

// Spark built-in partitioner
import org.apache.spark.Partitioner

// spark3d deps
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._

/**
  * Abstract class for partitioning the 3D space.
  * This class extends the Spark Partitioner class, and any new 3D partitioner
  * must extend in turn the SpatialPartitioner class.
  *
  * @param grids : (List[Shape3D])
  *   List of Shape3D objects which partition the space.
  *
  */
abstract class SpatialPartitioner(grids : List[Shape3D]) extends Partitioner with Serializable {
  /**
    * Method to place a spatialObject (could a Point3D, a Sphere, and so on) on
    * a grid. In practice it will assign a key (Int) to the spatialObject
    * corresponding the partition index.
    * This method must be specifically implemented for each class
    * extending SpatialPartitioner.
    *
    * @param spatialObject : (T<:Shape3D)
    *   Object of type T = Shape3D, or any extension like Point3D, Sphere, ...
    * @return (Iterator[Tuple2[Int, T]]) Iterator over
    *   a tuple (Key, Object). Key represents the partition number to which the
    *   spatialObject T belongs to.
    *
    */
  def placeObject[T<:Shape3D](spatialObject : T) : Iterator[Tuple2[Int, T]] = ???

  def placePoints(x: Double, y: Double, z: Double, isSpherical: Boolean) : Int = ???
  def placeSpheres(x: Double, y: Double, z: Double, r: Double, isSpherical: Boolean) : Int = ???

  /**
    * Method to return the index of a partition
    *
    * @param key : (Any)
    *   The Key of the partition (Key/Value)
    * @return (Int) The key of the partition as Int.
    */
  override def getPartition(key : Any) : Int = {
    key.asInstanceOf[Int]
  }

  /**
    * Gets the partitions which contain the input object.
    *
    * @param spatialObject input object for which the containment is to be found
    * @return list of Tuple of containing partitions and their index/partition IDs
    */
  def getPartitionNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = ???

  /**
    * Gets the partitions which are the neighbors of the partitions which contain the input object.
    *
    * @param spatialObject input object for which the neighbors are to be found
    * @param inclusive If true, includes the node of the spatialObject as well. Default is false.
    * @return list of Tuple of neighbor partitions and their index/partition IDs
    */
  def getNeighborNodes[T <: Shape3D](spatialObject: T, inclusive: Boolean = false): List[Tuple2[Int, Shape3D]] = ???

  /**
    * Gets the partitions which are the neighbors to the input partition. Useful when getting
    * secondary neighbors (neighbors to neighbor) of the queryObject.
    *
    * @param containingNode The boundary of the Node for which neighbors are to be found.
    * @param containingNodeID The index/partition ID of the containingNode
    * @return list of Tuple of secondary neighbor partitions and their index/partition IDs
    */
  def getSecondaryNeighborNodes[T <: Shape3D](containingNode: T, containingNodeID: Int): List[Tuple2[Int, Shape3D]] = ???
}
