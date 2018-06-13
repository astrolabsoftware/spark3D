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
package com.spark3d.geometryObjects

import com.spark3d.geometryObjects.Shape3D._

import scala.math._

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
    override val center: Point3D,
    var innerRadius: Double,
    var outerRadius: Double)
  extends Shape3D with Serializable {

  /**
    * Creates a null shell Envelope.
    */
  def this() {
    this(new Point3D(0.0, 0.0, 0.0, true), -1.0, -2.0)
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
    this(new Point3D(x, y, z, true), innerRadius, outerRadius)
  }

  /**
    * Creates a shell Envelope defined with a center coordinates, and a radius.
    * This would correspond to a Sphere basically.
    *
    * @param x x-coordinate of the center of the sphere Envelope
    * @param y y-coordinate of the center of the sphere Envelope
    * @param z z-coordinate of the center of the sphere Envelope
    * @param radius inner radius of the Envelope
    */
  def this(x: Double, y: Double, z: Double, radius: Double) {
    this(new Point3D(x, y, z, true), 0.0, radius)
  }

  /**
    * Creates a shell Envelope defined with a center coordinates, and a radius.
    * This would correspond to a Sphere basically.
    *
    * @param x x-coordinate of the center of the sphere Envelope
    * @param y y-coordinate of the center of the sphere Envelope
    * @param z z-coordinate of the center of the sphere Envelope
    * @param radius inner radius of the Envelope
    */
  def this(p: Point3D, radius: Double) {
    this(p, 0.0, radius)
  }

  /**
    * Clones the existing shell Envelope
    *
    * @param env shell Envelope to be cloned
    */
  def this(env: ShellEnvelope) {
    this(env.center, env.innerRadius, env.outerRadius)
  }

  /**
    * Returns if this is a null shell envelope.
    *
    * @return if this shell Envelope is null (empty Geometry) or not
    */
  def isNull(): Boolean = {
    return if (innerRadius < 0 || outerRadius < 0 || innerRadius > outerRadius) true else false
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

    4 * Pi * ((outerRadius * outerRadius) - (innerRadius * innerRadius))
  }

  /**
    * Expands the shell Envelope so that it contains the given Point. This will expand the only the inner
    * or outer radius based on which side of the shell the point belongs. The point is considered to be in
    * the shell if its distance to center is greater than or equal to the innerRadius and less than the
    * outerRadius (while doing OnionPartitioning similar convention is followed). So if we are expanding the
    * outer radius, a small buffer of 0.1 is added to ensure that point lies inside of the sphere defined by
    * the outerRadius and not onto it. Expansion here can be both positive and negative.
    *
    * @param p the Point to expand to include
    */
  def expandToInclude(p: Point3D): Unit = {
    if (isNull) {
      return
    }

    val delta = p.distanceTo(center)

    if (delta < outerRadius && delta >= innerRadius) {
      return
    } else if (delta < innerRadius) {
      innerRadius = delta
    } else if (delta >= outerRadius) {
      outerRadius = delta + 0.1
    }
  }

  /**
    * Expands the shell Envelope so that it includes the other shell Envelope. The Envelopes have to be concentric.
    * Either inner or outer or both radii will be expanded based on following criteria -
    *
    * Expand inner radius-
    *   - When the outer and inner radii of the input shell Envelope are strictly less than the outer and inner
    *     radii of this shell Envelope
    *     sphere and inner radius of the inout sphere is less
    *
    * Expand outer radius-
    *   - When the outer radius of the input shell Envelope is greater than or equal to this shell Envelope. We add a
    *     small buffer of 0.1 to the outer radius in this case to ensure that the outer radii of the two shell
    *     Envelopes are not equal for consistency in onion Partitioning code. Expansion here can be both positive and
    *     negative.
    * Expand both radio -
    *   - Condition for the epansion of the outer radius and when the inner radius of the input shell Envelope is
    *     strictly less than than the inner radius of this shell Envelope
    *
    * @param spr the shell Envelope to be included
    */
  def expandToInclude(spr: ShellEnvelope): Unit = {

    if (isNull || spr.isNull) {
      return
    }

    if(!center.isEqual(spr.center)) {
      throw new AssertionError(
        """
        The two shells must be centered on the same point!
        """)
    }

    if ((spr.outerRadius < outerRadius) && (spr.innerRadius < innerRadius)) {
      innerRadius = spr.innerRadius
    } else if (spr.outerRadius >= outerRadius) {
      if (spr.innerRadius < innerRadius) {
        innerRadius = spr.innerRadius
      }
      outerRadius = spr.outerRadius + 0.1
    }
  }


  /**
    * Expand shell Envelope by given distance. This will increase both inner and outer radius
    * by input distance.
    *
    * @param delta the distance to expand the shell Envelope
    */
  def expandBy(delta: Double): Unit = {
      if (isNull) {
        return
      }

      outerRadius += delta
      innerRadius += delta
    }

  /**
    * Expand the inner radius of the shell by given distance. If inner radius becomes greater than the outer radius
    * after this we set the shell Envelope to null
    *
    * @param delta the distance to expand the inner radius of the shell Envelope by
    */
  def expandInnerRadius(delta: Double): Unit = {
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
  def expandOuterRadius(delta: Double): Unit = {
    if(isNull) {
      return
    }

    outerRadius += delta
  }

  /**
    * Checks if the region of the input shell Envelope intersects the region of this shell Envelope.
    * The case where one shell Envelope lies completely within the another shell Envelope is considered as
    * non-intersecting.
    *
    * @param spr the shell Envelope with which the intersection is being checked
    * @return true if the one shell Envelope intersects the other
    */
  def intersects(spr: ShellEnvelope): Boolean = {
    if (isNull || spr.isNull) {
      return false
    }

    val centerDist = center.distanceTo(spr.center)

    // the case where one shell Envelope lies completely within the another shell Envelope
    if ((centerDist + spr.outerRadius < innerRadius) || (centerDist + outerRadius < spr.innerRadius)) {
      return false
    }


    if (centerDist <= (outerRadius + spr.outerRadius)) {
      return true
    }

    false
  }

  /**
    * Check whether a point belong to a shell Envelope.
    * If a point lies on the sphere defined by inner radius (not outer radius),
    * it is considered to be belonging to the shell for the consistency with the onion partitioning code.
    *
    * @param p the point for which the containment is to be checked
    * @return true if the shell Envelope contains the point
    */
  def isPointInShell(p: Point3D): Boolean = {
    if (isNull) {
      return false
    }

    val dist = center.distanceTo(p)

    if ((dist >= innerRadius) && (dist < outerRadius)) {
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

  /**
    * Get the bounding box of the Sphere
    *
    * @return bounding box (Cuboid) of the Sphere
    */
  override def getEnvelope: BoxEnvelope = {
    BoxEnvelope.apply(
        center.x - outerRadius, center.x + outerRadius,
        center.y - outerRadius, center.y + outerRadius,
        center.z - outerRadius, center.z + outerRadius)
  }
}


object ShellEnvelope {

  /**
    * Check whether a point belong to a shell created by the difference of the two radii.
    * If a point lies on the sphere defined by inner radius (not outer radius), it is considered to be in be
    * belonging to the shell for the consistency with the onion partitioning code.
    *
    * @param innerRadius inner radius of the shell
    * @param outerRadius outer radius of the shell
    * @param center center of the two spheres which are defined by the inner and outer radius
    * @param p Point3D to be checked for containment
    * @return true if the point belongs to the shell defined by two radii, false otherewise or when the
    *         shell is invalid
    */
  def isPointInShell(innerRadius: Double, outerRadius: Double, center: Point3D, p: Point3D): Boolean = {
    if (innerRadius > outerRadius) {
      return false
    }

    val dist = center.distanceTo(p)

    if ((dist >= innerRadius) && (dist < outerRadius)) {
      return true
    }

    false
  }
}
