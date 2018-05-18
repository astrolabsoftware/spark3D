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
package com.spark3d.utils

import com.spark3d.geometryObjects._

object Utils {

  /**
    * Convert a Point3D with euclidean coordinates in a
    * Point3D with spherical coordinates.
    *
    * @param p : (Point3D)
    *   Input Point3D with euclidean coordinates.
    * @return (Point3D) The same point but with spherical coordinates.
    */
  def euclideantoSpherical(p : Point3D) : Point3D = {
    if (p.spherical) {
      throw new AssertionError("""
        Cannot convert your point to spherical coordinates because
        it is already in spherical coordinates.""")
    }

    val r = math.sqrt(p.x*p.x + p.y*p.y + p.z*p.z)
    val theta = math.acos(p.z / r)
    val phi = math.atan(p.y / p.x)

    // Return the new point in spherical coordinates
    new Point3D(r, theta, phi, true)
  }

  /**
    * Convert a Point3D with spherical coordinates in a
    * Point3D with euclidean coordinates.
    *
    * @param p : (Point3D)
    *   Input Point3D with spherical coordinates.
    * @return (Point3D) The same point but with euclidean coordinates.
    */
  def sphericalToEuclidean(p : Point3D) : Point3D = {
    if (!p.spherical) {
      throw new AssertionError("""
        Cannot convert your point to euclidean coordinates because
        it is already in euclidean coordinates.""")
    }

    val x = p.x * math.sin(p.y) * math.cos(p.z)
    val y = p.x * math.sin(p.y) * math.sin(p.z)
    val z = p.x * math.cos(p.y)

    // Return the new point in spherical coordinates
    new Point3D(x, y, z, false)
  }
}
