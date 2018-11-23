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

import org.apache.spark.rdd.RDD

import com.astrolabsoftware.spark3d.geometryObjects.ShellEnvelope
import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._

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
    * Elements of the Grid as List[ShellEnvelope]
    */
  val grids = List.newBuilder[ShellEnvelope]

  /**
    * Get the highest radial coordinate value (max(Z)).
    *
    * @param rdd (RDD[T])
    *   RDD of type T (which must extends Shape3D)
    * @return (Double) the highest radial coordinate value (max(Z)).
    */
  def getMaxZ[T<:Shape3D](rdd : RDD[T]) : Double = {
    rdd.map(x => x.center.distanceTo(new Point3D(0.0, 0.0, 0.0, x.center.isSpherical))).max
  }

  /**
    * Initialise a new Onion space, by linearly slicing the radial coordinate.
    * The space is made of shells, that is each partition is the
    * difference between two subsequent spheres.
    * The resolution of the space, that is the radial distance between
    * two partitions is given by the highest radial coordinate value divided by
    * the number of partitions.
    *
    * @param numPartitions : (Int)
    *   Number RDD partitions
    * @param maxZ : (Double)
    *   Highest radial coordinate value for our space.
    * @param isSpherical : (Boolean)
    *   True if the coordinate system of the data is spherical (r, theta, phi).
    *   False if the coordinate system of the data is cartesian.
    */
  def LinearOnionPartitioning(
      numPartitions : Int, maxZ : Double, isSpherical: Boolean) : Unit = {

    // The resolution of the space
    val dZ : Double = maxZ / numPartitions

    val center = new Point3D(0.0, 0.0, 0.0, isSpherical)

    // Add shells
    for (pos <- 0 to numPartitions - 1) {

      val shell = if (pos == numPartitions - 1) {
        // Expand by 10% to get points lying on the last outer shell.
        new ShellEnvelope(center, pos * dZ, (pos+1) * dZ * 1.1)
      } else {
        new ShellEnvelope(center, pos * dZ, (pos+1) * dZ)
      }

      grids += shell
    }
  }

  /**
    * Returns the n Shells required to build the n grid elements.
    *
    * @return (List[ShellEnvelope]) List of n Shells.
    *
    */
  def getGrids : List[ShellEnvelope] = {
    this.grids.result
  }
}
