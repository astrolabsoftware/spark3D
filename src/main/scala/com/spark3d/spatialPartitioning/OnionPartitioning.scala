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

import com.spark3d.geometryObjects.Sphere

/**
  * Class to deal with the onion space.
  * The idea is described here:
  * https://github.com/JulienPeloton/spark3D/issues/11
  *
  * The difference between 2 concentric spheres will define the
  * elements of the grid (Spark partitions) such that we will have a onion space!
  *
  */
class OnionPartitioning extends Serializable {

  /**
    * Elements of the Grid as List[Sphere]
    */
  val grids = List.newBuilder[Sphere]

  /**
    * Initialise a new Onion space, by linearly slicing the radial coordinate.
    * The space is made of concentric spheres, and each partition is the
    * difference between two subsequent spheres.
    *
    * @param minZ : (Double)
    *   Smallest radial coordinate value for our space.
    * @param maxZ : (Double)
    *   Highest radial coordinate value for our space.
    * @param dZ : (Double)
    *   Resolution of the space, that is radial distance between two partitions.
    */
  def LinearOnionPartitioning(minZ : Double, maxZ : Double, dZ : Double) : Unit = {

    // Number of Spark partitions in our space
    // This is just a linear split of the radial coordinate.
    val npd : Double = (maxZ - minZ) / dZ

    // Add one more partitions if the result is not an integer
    val num_partitions = if (npd % npd.toInt == 0) {
      npd.toInt
    } else {
      npd.toInt + 1
    }

    // Add concentric spheres. Note that there is n+1 spheres as
    // shells are created by the difference between 2 subsequent spheres
    // (n+1 spheres give n shells).
    for (pos <- 0 to num_partitions) {
      val sphere = new Sphere(0.0, 0.0, 0.0, dZ * pos)
      grids += sphere
    }
  }

  /**
    * Returns the n+1 Spheres required to build the n grid elements.
    *
    * @return (List[Sphere]) List of n+1 Spheres.
    *
    */
  def getGrids : List[Sphere] = {
    this.grids.result
  }
}
