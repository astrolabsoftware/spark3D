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
  * Generic objects describing 3D shapes.
  * Other 3D shapes must extend this.
  */
object Shape3D extends Serializable {

  /**
    * Generic methods for 3D shapes
    */
  trait Shape3D {

    /**
      * The center
      */
    val center : Point3D

    /**
      * Radius of the Shape.
      * Only meaningful for Sphere.
      */
    val radius : Double

    /**
      * Whether two shapes intersect each other.
      *
      * @param otherShape : (Geometry3D)
      *   Other instance of Geometry3D
      * @return (Boolean) True if they overlap. False otherwise.
      */
    def intersect(otherShape: Shape3D) : Boolean

    /**
      * Compute the volume of the 3D shape.
      *
      * @return (Double) the volume of the shape.
      */
    def getVolume : Double
  }

  /**
    * Intersection between two spheres. We compare the distance between
    * the two centers with the sum of the two radii.
    * Work also with points (sphere with zero radius).
    *
    * @param sphere1 : (Shape3D)
    *   Instance of Shape3D, and more specifically Sphere.
    * @param sphere2 : (Shape3D)
    *   Instance of Shape3D, and more specifically Sphere.
    */
  def sphereSphereIntersection(sphere1: Shape3D, sphere2: Shape3D) : Boolean = {

    // Quick check on the types of objects
    if (!sphere1.isInstanceOf[Sphere] && !sphere1.isInstanceOf[Point3D]) {
      throw new AssertionError("""
        You are using sphereSphereIntersection with a non-spherical object
        """)
    }

    if (!sphere2.isInstanceOf[Sphere] && !sphere2.isInstanceOf[Point3D]) {
      throw new AssertionError("""
        You are using sphereSphereIntersection with a non-spherical object
        """)
    }

    // Compute the distance between the two centers
    val distance = sphere1.center.distanceTo(sphere2.center)

    // Compute the sum of the two sphere radii
    val sumRadii = sphere1.radius + sphere2.radius

    // Compare the distance between centers, and the radius sum.
    if (sumRadii >= distance) {
      true
    } else false
  }

  /**
    * Check whether a point belong to a spherical shell made by the difference
    * of two spheres.
    *
    * @param lower_sphere : (Sphere)
    *   Lower sphere defining the lower bound of the shell (included).
    * @param upper_sphere : (Sphere)
    *   Upper sphere defining the upper bound of the shell (excluded).
    * @param p : (Point3D)
    *   Point of the space
    * @return (Boolean) True if the point is between the two sphere.
    *
    */
  def isPointInShell(lower_sphere : Sphere, upper_sphere : Sphere, p : Point3D): Boolean = {

    if (lower_sphere.center.getCoordinate != upper_sphere.center.getCoordinate) {
      throw new AssertionError("""
        The two spheres must be centered on the same point!
        """)
    }

    // Distance to the center
    val distance = math.sqrt(p.x * p.x + p.y * p.y + p.z * p.z)

    // Whether the point is in between the two spheres.
    if (distance >= lower_sphere.radius && distance < upper_sphere.radius) {
      true
    } else false
  }
}
