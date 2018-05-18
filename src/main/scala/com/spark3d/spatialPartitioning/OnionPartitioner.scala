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

import collection.JavaConverters._

import scala.collection.mutable.HashSet

import com.spark3d.utils.GridType._
import com.spark3d.spatialPartitioning
import com.spark3d.geometryObjects.Sphere
import com.spark3d.geometryObjects.Shape3D._

/**
  * Class extending SpatialPartitioner to deal with the onion space.
  * The idea is described here:
  * https://github.com/JulienPeloton/spark3D/issues/11
  *
  * The difference between 2 concentric spheres will define the
  * elements of the grid (Spark partitions) such that we will have a onion space!
  *
  * @param gridType : (GridType)
  *   One of the available grid type listed in GridType.
  * @param grids : (List[Sphere])
  *   List of concentric spheres which partition the space. Radius of the
  *   spheres must be increasing.
  *
  */
class OnionPartitioner(gridType : GridType, grids : List[Sphere]) extends SpatialPartitioner(gridType, grids) {

  /**
    * The number of partitions is the number of sphere as grid elements
    * are difference between 2 concentric spheres. The first (n-1) partitions
    * will contain the objects lying in our Onion space. The nth partition will
    * host all points lying outside the space.
    *
    * @return (Int) the number of partition
    */
  override def numPartitions : Int = {
    grids.size
  }

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
    * @return (java.util.Iterator[Tuple2[Int, T]]) Java iterator over a Tuple
    *   of (Int, T) where Int is the partition number, and T the input object.
    *
    */
  override def placeObject[T<:Shape3D](spatialObject : T) : java.util.Iterator[Tuple2[Int, T]] = {

    // Grab the center of the geometrical objects
    val center = spatialObject.center
    var containFlag : Boolean = false
    val notIncludedID = grids.size
    val result = HashSet.empty[Tuple2[Int, T]]

    for (pos <- 0 to grids.size - 2) {
      val lower_sphere = grids(pos)
      val upper_sphere = grids(pos + 1)

      if (isPointInShell(lower_sphere, upper_sphere, center)) {
        result += new Tuple2(pos, spatialObject)
        containFlag = true
      }
    }

    if (!containFlag) {
      result += new Tuple2(notIncludedID, spatialObject)
    }

    // Make it Java for GeoSpark compatibility
    result.toIterator.asJava
  }

}
