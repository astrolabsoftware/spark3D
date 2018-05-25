/*
 * Copyright 2018 Mayur Bhosale
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
package com.spark3d.geometry

import com.spark3d.geometryObjects._

/** Defines a shell of 3D coordinate space. Shell here is made by a difference of
  * of two two concentric spheres. This can be used to define a bounding box of a geometryObject
  *
  * An Shell Envelope can be uniquely defined based on the center and its outer and inner radius.
  *
  * @param center coordinates of the center of the shell Envelope
  * @param innerRadius inner radius of the shell Envelope
  * @param outerRadius outer radius of the shell Envelope
  */
class ShellEnvelope(
    var center: Point3D,
    var innerRadius: Double,
    var outerRadius: Double)
  extends Serializable {

  /**
    * Creates a null shell Envelope.
    */
  def this() {
    this(new Point3D(0.0, 0.0, 0.0), -1.0, -2.0)
  }


  /**
    * Creates a shell Envelope defined with a center coordinates, inner and outer radius.
    *
    * @param x x-coordinate of the center of the sphere Envelope
    * @param y y-coordinate of the center of the sphere Envelope
    * @param z z-coordinate of the center of the sphere Envelope
    * @param innerRadius inner radius of the Envelope
    * @param outerRadius outer radius of the Envelope
    */
  def this(x: Double, y: Double, z: Double, innerRadius: Double, outerRadius: Double) {
    this(new Point3D(x, y, z), innerRadius, outerRadius)
  }

  /**
    * Returns if this is a null shell envelope.
    *
    * @return if this shell Envelope is null (empty Geometry) or not
    */
  def isNull(): Boolean = {
    return if (innerRadius < 0 || outerRadius < 0 || innerRadius > outerRadius) false else true
  }

  /**
    * Sets this shell Envelope to a null.
    *
    */
  def setToNull: Unit = {
    innerRadius = -1
    outerRadius = -1
  }

  /**
    * Returns the area of the shell Envelope.
    *
    * @return the area of the shell envelope, 0.0 if it is null
    */
  def getArea(): Double = {

    if (isNull) {
      return 0.0
    }

    val pi = 3.14159
    4 * pi * ((outerRadius * outerRadius) - (innerRadius * innerRadius))
  }

  /**
    * Expands the shell Envelope so that it contains the given Point
    *
    * @param p the Point to expand to include
    */
  def expandToInclude(p: Point3D): Unit = {
    if (isNull()) {
      return
    }

    val delta = p.distanceTo(center)
    expandBy(delta - outerRadius)
  }

  /**
    * Expands the shell Envelope so that it includes the other shell Envelope.
    * This will expand this Envelope till the outer radius boundary of the other Envelope
    *
    * @param spr the shell Envelope to be included
    */
  def expandToInclude(spr: ShellEnvelope): Unit = {

    if (spr.isNull) {
      return
    }

    val delta = spr.center.distanceTo(center)
    expandBy(delta - outerRadius + spr.outerRadius)
  }


  /**
    * Expand shell Envelope by given distance. This will increase both inner and outer radius
    * by input distance.
    *
    * @param delta the distance to expand the shell Envelope
    */
  def expandBy(delta: Double): Unit = {
    if (isNull()) {
      return
    }

    outerRadius += delta
    innerRadius += delta
  }

  /**
    * Expand the inner radius of the shell by given distance.
    *
    * @param delta the distance to expand the inner radius of the shell Envelope by
    */
  def expandInnerSphere(delta: Double): Unit = {
    if(isNull) {
      return
    }

    innerRadius += delta

    if (isNull) {
      setToNull
    }
  }

  /**
    * Expand the outer radius of the shell by given distance.
    *
    * @param delta the distance to expand the outer radius of the shell Envelope by
    */
  def expandOuterSphere(delta: Double): Unit = {
    if(isNull) {
      return
    }

    outerRadius += delta
  }

  /**
    * Checks if the region of the input shell Envelope intersects the region of this shell Envelope.
    *
    * @param spr the shell Envelope with which the intersection is being checked
    * @return true if the one shell Envelope intersects the other
    */
  def intersects(spr: ShellEnvelope): Boolean = {
    if (isNull || spr.isNull) {
      return false
    }

    val centerDist = center.distanceTo(spr.center)

    // the case where one sphere lies completely within the another sphere
    if ((centerDist + spr.outerRadius < innerRadius) || (centerDist + outerRadius < spr.innerRadius)) {
      return false
    }


    if (centerDist <= (outerRadius + spr.outerRadius)) {
      return true
    }

    false
  }

  /**
    * Check whether a point belong to a shell Envelope
    *
    * @param p the point for which the containment is to be checked
    * @return true if the shell Envelope contains the point
    */
  def isPointInShell(p: Point3D): Boolean = {
    if (isNull) {
      return false
    }

    val dist = center.distanceTo(p)

    if ((dist >= innerRadius) && (dist <= outerRadius)) {
      return true
    }

    false
  }

  /**
    * Checks if the region of the input shell Envelope is contained by this shell Envelope
    *
    * @param spr the shell Envelope for which the containment is to be checked
    * @return true if the shell Envelope completely contains the input shell Envelope
    */
  def contains(spr: ShellEnvelope): Boolean = {
    if (isNull || spr.isNull) {
      return false
    }

    val dist = center.distanceTo(spr.center)

    dist + spr.outerRadius <= innerRadius
  }

  /**
    * Checks if the two shell Envelopes are equal.
    *
    * @param spr input sphere Envelope for which the equality is to be checked
    * @return true if the two sphere Envelopes are equal
    */
  def isEqual(spr: ShellEnvelope): Boolean = {
    if (isNull || spr.isNull) {
      return false
    }

    center.isEqual(spr.center) && (innerRadius == spr.innerRadius) && (outerRadius == spr.outerRadius)
  }
}
