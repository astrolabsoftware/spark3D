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
  * elements of the grid (from the partition point of view).
  * In a sense, we will have a onion space!
  * For each elements, the objects will be projected into shells,
  * and the surface will be split using Healpix.
  *
  */
class OnionPartitioning extends Serializable {

  // Elements of the grid
  val grids = List.newBuilder[Sphere]

  /**
    * Instantiate a new Onion space
    *
    * @param minZ : (Double)
    * @param maxZ : (Double)
    * @param dZ : (Double)
    */
  def OnionPartitioning(minZ : Double, maxZ : Double, dZ : Double) : Unit = {

    // Number of Spark partition in our space
    // This is just a linear split of the Z coordinate.
    val npd : Double = (maxZ - minZ) / dZ
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
