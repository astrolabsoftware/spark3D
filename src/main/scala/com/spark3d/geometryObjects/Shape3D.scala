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
}
