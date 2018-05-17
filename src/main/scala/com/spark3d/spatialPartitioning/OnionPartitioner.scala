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

// import java.util.HashSet
import scala.collection.mutable.HashSet

import com.spark3d.geometryObjects.Sphere

import com.spark3d.spatialPartitioning
import com.spark3d.utils.GridType._
import com.spark3d.geometryObjects.Sphere
import com.spark3d.geometryObjects.Point3D
import com.spark3d.geometryObjects.Shape3D._

/**
  * Class to deal with the onion space.
  * The idea is described here:
  * https://github.com/JulienPeloton/spark3D/issues/11
  *
  * The difference between 2 concentric spheres will define the
  * elements of the grid (from the partition point of view).
  * In a sense, we will have a onion space!
  * For each elements, the objects will be projected into shells,
  * and the surface will be split using Healpix.
  *
  */
class OnionPartitioner(gridType : GridType, grids : List[Sphere]) extends SpatialPartitioner(gridType, grids) {

  /**
    * The number of partitions is the number of sphere - 1 as grid elements
    * are difference between 2 concentric spheres.
    *
    * @return (Int) the number of partition
    */
  override def numPartitions : Int = {
    grids.size - 1
  }

  /**
    * Associate geometrical objects (Point3D, Sphere, etc) to grid elements of
    * the onion space.
    */
  override def placeObject[T<:Shape3D](spatialObject : T) : Iterator[Tuple2[Int, T]] = {

    val center = spatialObject.center
    var containFlag : Boolean = false
    val notIncludedID = -1
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

    result.toIterator
  }

}
