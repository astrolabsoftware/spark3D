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
package com.spark3d.geometryObjects

/**
  * Class for describing a point in 3D space.
  */
class Point3D(val x: Double, val y: Double, val z: Double) {

  /**
    * Returns the distance between the point and another.
    * Space is supposed flat (euclidean).
    *
    * @param p : (Point3D)
    *   Another instance of Point3D
    * @return (Double) Distance between the two points.
    *
    */
  def distanceTo(p : Point3D) : Double = {
    val module = math.sqrt(
      (this.x - p.x)*(this.x - p.x) +
      (this.y - p.y)*(this.y - p.y) +
      (this.z - p.z)*(this.z - p.z))
    module
  }

  /**
    * Return the coordinates (x, y, z) of the point.
    *
    * @return (List[Double]) The list of coordinates.
    */
  def getCoordinate : List[Double] = {
    List(this.x, this.y, this.z)
  }
}
