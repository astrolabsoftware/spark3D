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

// import collection.JavaConverters._

// Scala deps
import scala.util.control.Breaks._
import scala.collection.mutable.HashSet

// spark3d deps
import com.spark3d.geometry.ShellEnvelope
import com.spark3d.spatialPartitioning
import com.spark3d.geometryObjects.Shape3D._


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
    * @return (Int) the number of partition
    */
  override def numPartitions : Int = {
    grids.size
  }

  /**
    * Hashcode returns the number of partitions.
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
}
