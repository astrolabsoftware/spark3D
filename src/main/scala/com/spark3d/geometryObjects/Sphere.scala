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

import com.spark3d.geometry.BoxEnvelope
import com.spark3d.geometryObjects.Shape3D._

/**
  * Class to handle a Sphere
  *
  * @param x : (Double)
  *   X coordinate of the center
  * @param y : (Double)
  *   Y coordinate of the center
  * @param z : (Double)
  *   Z coordinate of the center
  * @param radius : (Double)
  *   Radius of the Sphere
  */
class Sphere(val x: Double, val y: Double, val z: Double, override val radius: Double) extends Shape3D with Serializable {

  // Center of our Sphere
  val center = new Point3D(this.x, this.y, this.z, true)

  /**
    * Methods to determine whether two shapes overlap.
    * Implement different ways for different shapes.
    *
    * @param otherShape : (Shape3D)
    *   An instance of Shape3D (or extension)
    * @return (Boolean) true if the two objects intersect.
    *
    */
  override def intersect(otherShape : Shape3D): Boolean = {

    // Different methods to handle different shapes
    // Keep in mind a point is a sphere with radius 0.
    if (otherShape.isInstanceOf[Sphere] | otherShape.isInstanceOf[Point3D]) {
      sphereSphereIntersection(this, otherShape)
    } else {
      throw new AssertionError("""
        Cannot perform intersection because the type of shape is unknown!
        Currently implemented:
          - sphere x sphere
          - sphere x point
          - point  x point
        """)
    }
  }

  /**
    * Volume of a Sphere.
    * @return (Double) 4/3 * pi * R**3
    *
    */
  override def getVolume : Double = {
    4.0 / 3.0 * math.Pi * this.radius * this.radius * this.radius
  }

  /**
    *
    * @return
    */
  override def getEnvelope: BoxEnvelope = {
    BoxEnvelope.apply(
        center.x - radius, center.x + radius,
        center.y - radius, center.y + radius,
        center.z - radius, center.z + radius)
  }
}
