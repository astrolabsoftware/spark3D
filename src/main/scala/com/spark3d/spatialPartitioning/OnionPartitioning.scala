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

import org.apache.spark.rdd.RDD

import com.spark3d.geometryObjects.Sphere
import com.spark3d.geometryObjects.Point3D
import com.spark3d.geometryObjects.Shape3D._

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
    * Get the highest radial coordinate value (max(Z)).
    *
    * @param rdd (RDD[T])
    *   RDD of type T (which must extends Shape3D)
    * @return (Double) the highest radial coordinate value (max(Z)).
    */
  def getMaxZ[T<:Shape3D](rdd : RDD[T]) : Double = {
    rdd.map(x => x.center.distanceTo(new Point3D(0.0, 0.0, 0.0))).max
  }

  /**
    * Initialise a new Onion space, by linearly slicing the radial coordinate.
    * The space is made of concentric spheres, and each partition is the
    * difference between two subsequent spheres.
    * The resolution of the space, that is the radial distance between
    * two partitions is given by the highest radial coordinate value divided by
    * the number of partitions.
    *
    * @param numPartitions : (Int)
    *   Number RDD partitions
    * @param maxZ : (Double)
    *   Highest radial coordinate value for our space.
    */
  def LinearOnionPartitioning(numPartitions : Int, maxZ : Double) : Unit = {

    // The resolution of the space
    val dZ : Double = maxZ / numPartitions

    // Add concentric spheres. Note that there is n+1 spheres as
    // shells are created by the difference between 2 subsequent spheres
    // (n+1 spheres give n shells).
    for (pos <- 0 to numPartitions) {
      val sphere = if (pos == numPartitions) {
        // Pad the boundary to include points at the boundaries.
        new Sphere(0.0, 0.0, 0.0, 1.1 * dZ * pos)
      } else {
        new Sphere(0.0, 0.0, 0.0, dZ * pos)
      }
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
