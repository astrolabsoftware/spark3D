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

import scala.math._
import com.spark3d.geometryObjects._

/** Defines a spherical/cubical region of 3D coordinate space.
  * This can be used to define a bounding box of a geometryObject
  *
  * An Envelope can be uniqely defined based on minimum and maximum coordinates along all
  * 2 axes. On creating the Envelope initially, the min's and max's are setted automatically.
  *
  * Default constructor is kept private to avoid creating an instance of the Envelope class without initialising
  * the min/max coordinates along axes incorrectly.
  *
  * @param minX minimum coordinate of Envelope along X-axis
  * @param maxX maximum coordinate of Envelope along X-axis
  * @param minY minimum coordinate of Envelope along Y-axis
  * @param maxY maximum coordinate of Envelope along Y-axis
  * @param minZ minimum coordinate of Envelope along Z-axis
  * @param maxZ maximum coordinate of Envelope along Z-axis
  */
class Envelope private (
    var minX: Double, var maxX: Double,
    var minY: Double, var maxY: Double,
    var minZ: Double, var maxZ: Double)
  extends Serializable {

  /**
    * Creates a null Envelope
    */
  def this() {
    this(0.0, -1.0, 0.0, -1.0, 0.0, -1.0)
  }

  /**
    * Creates an Envelope for a region defined by three coordinates.
    *
    * @param p1 first coordinate
    * @param p2 second coordinate
    * @param p3 third coordinate
    */
  def this(p1: Point3D, p2: Point3D, p3: Point3D) {
    this(
      min(p1.x, min(p2.x, p3.x)), max(p1.x, max(p2.x, p3.x)),
      min(p1.y, min(p2.y, p3.y)), max(p1.y, max(p2.y, p3.y)),
      min(p1.z, min(p2.z, p3.z)), max(p1.z, max(p2.z, p3.z))
    )
  }

  /**
    * Creates an Envelope for a region defined by two coordinates.
    *
    * @param p1 first coordinate
    * @param p2 second coordinate
    */
  def this(p1: Point3D, p2: Point3D) {
    this(
      min(p1.x, p2.x), max(p1.x, p2.x),
      min(p1.y, p2.y), max(p1.y, p2.y),
      min(p1.z, p2.z), max(p1.z, p2.z)
    )
  }

  /**
    * Creates an Envelope for a region defined one coordinate. The Envelope in this case will also be a point.
    *
    * @param p1 the coordinate
    */
  def this(p1: Point3D) {
    this(p1.x, p1.x, p1.y, p1.y, p1.z, p1.z)
  }

  /**
    * Clones an existing Envelope to create a duplicate Envelope.
    *
    * @param env original Envelope to be cloned
    */
  def this(env: Envelope) {
    this(env.minX, env.maxX, env.minY, env.maxY, env.minZ, env.maxZ)
  }

  /**
    * Returns of this is a null envelope.
    *
    * @return if this Envelope is null (empty Geometry) or nots
    */
  def isNull(): Boolean = {
    ((minX > maxX) || (minY > maxY) || (minZ > maxZ))
  }


  /**
    * Sets this Envelope to a null Envelope.
    *
    */
  def setToNull: Unit = {
    minX = 0
    maxX = -1
    minY = 0
    maxY = -1
    minZ = 0
    maxZ = -1
  }

  /**
    * Return the difference between max and min X values of the Envelope.
    *
    * @return maxX - minX, or 0 if the Envelope is null
    */
  def getXLength(): Double = {

    if (isNull) {
      return 0.0
    }
    maxX - minX
  }

  /**
    * Return the difference between max and min Y value of the Envelope.
    *
    * @return maxX - minY, or 0 if the Envelope is null
    */
  def getYLength(): Double = {

    if (isNull) {
      return 0.0
    }
    maxY - minY
  }

  /**
    * Return the difference between max and min Z value of the Envelope.
    *
    * @return maxZ - minZ, or 0 if the Envelope is null
    */
  def getZLength(): Double = {

    if (isNull) {
      return 0.0
    }
    maxZ - minZ
  }

  /**
    * Gets minimum extent of this Envelope across all three dimensions.
    *
    * @return the minimum extent of this Envelope
    */
  def minExtent(): Double = {
    if (isNull) {
      return 0.0
    }

    min(getXLength(), min(getYLength(), getZLength()))
  }

  /**
    * Gets maximum extent of this Envelope across all three dimensions.
    *
    * @return the maximum extent of this Envelope
    */
  def maxExtent(): Double = {
    if (isNull) {
      return 0.0
    }

    max(getXLength(), max(getYLength(), getZLength()))
  }

  /**
    * Returns the area of the Envelope.
    *
    * @return the area od the envelope, 0.0 if the Envelope is null
    */
  def getArea(): Double = {
    getXLength() * getYLength() * getZLength()
  }


  /**
    * Expand the Envelope so that it contains the given Point
    *
    * @param p the Point to expand to include
    */
  def expandToInclude(p: Point3D): Unit = {
    expandToInclude(p.x, p.y, p.z)
  }

  /**
    * Expand Envelope by given distance along the all three dimensions.
    *
    * @param delta the distance to expand the Envelope along all the axes
    */
  def expandBy(delta: Double): Unit = {
    expandBy(delta, delta, delta)
  }

  /**
    * Expand Envelope by given distances along the three dimension.
    *
    * @param deltaX the distance to expand the Envelope along the the X axis
    * @param deltaY the distance to expand the Envelope along the the Y axis
    * @param deltaZ the distance to expand the Envelope along the the Z axis
    */
  def expandBy(deltaX: Double, deltaY: Double, deltaZ: Double): Unit = {
    if (isNull) {
      return
    }

    minX -= deltaX
    maxX += deltaX
    minY -= deltaY
    maxY += deltaY
    minZ -= deltaZ
    maxZ += deltaZ

    // check if the envelope existence conditions are still valid after the expansions
    if ((minX > maxX) || (minY > maxY) || (minZ > maxZ)) {
      setToNull
    }
  }

  /**
    * Enlarges this Envelope so that it contains the given point.
    * Has no effect if the point is already on or within the evelope.
    *
    * @param x the value to lower the minimum x to or to raise the maximum x to
    * @param y the value to lower the minimum y to or to raise the maximum y to
    * @param z the value to lower the minimum z to or to raise the maximum z to
    */
  def expandToInclude(x: Double, y: Double, z: Double): Unit = {
    if (isNull) {
      minX = x
      maxX = x
      minY = y
      maxY = y
      minZ = z
      maxZ = z
    } else {
      if (x < minX) {
        minX = x
      }
      if (x > maxX) {
        maxX = x
      }
      if (y < minY) {
        minY = y
      }
      if (y > maxY) {
        maxY = y
      }
      if (z < minZ) {
        minZ = z
      }
      if (z > maxZ) {
        maxZ = z
      }
    }
  }

  /**
    * Expand the Envelope so that it includes the other Envelope.
    *
    * @param env the Envelope to expand to include
    */
  def expandToInclude(env: Envelope): Unit = {

    if (env.isNull) {
      return
    }

    if (isNull) {
      minX = env.minX
      maxX = env.maxX
      minY = env.minY
      maxY = env.maxY
      minZ = env.minZ
      maxZ = env.maxZ
    } else {
      if (env.minX < minX) {
        minX = env.minX
      }
      if (env.maxX > maxX) {
        maxX = env.maxX
      }
      if (env.minY < minY) {
        minY = env.minY
      }
      if (env.maxY > maxY) {
        maxY = env.maxY
      }
      if (env.minZ < minZ) {
        minZ = env.minZ
      }
      if (env.maxZ > maxZ) {
        maxZ = env.maxZ
      }
    }
  }

  /**
    * Translates/move this envelope by given amounts in the X, Y and Z direction.
    *
    * @param transX the amount to translate along the X axis
    * @param transY the amount to translate along the Y axis
    * @param transZ the amount to translate along the Z axis
    */
  def translate(transX: Double, transY: Double, transZ: Double): Unit = {
    if (isNull) {
      return
    }

    minX += transX
    maxX += transX
    minY += transY
    maxY += transY
    minZ += transZ
    maxZ += transZ
  }

  /**
    * Computes the coordinate of the centre of this envelope (as long as it is non-null
    *
    * @return he centre coordinate of this envelope, null if the Envelope is null
    */
  def center(): Point3D = {
    if (isNull) {
      return null
    }

    new Point3D((minX + maxX) / 2.0,
      (minY + maxY) / 2.0,
      (minZ + maxZ) / 2.0)
  }

  /**
    * Comptutes the intersection of the two Envelopes/
    *
    * @param env the envelope to find intersection with
    * @return a new Envelope representing the intersection of the envelopes (this will be
    * the null envelope if either if the envelopes is null, or they do not intersect
    */
  def intersection(env: Envelope): Envelope = {
    if (isNull || env.isNull) {
      return null
    }

    val intMinX = if (minX < env.minX) minX else env.minX
    val intMaxX = if (maxX < env.maxX) maxX else env.maxX
    val intMinY = if (minY < env.minY) minY else env.minY
    val intMaxY = if (maxY < env.maxY) maxY else env.maxY
    val intMinZ = if (minZ < env.minZ) minZ else env.minZ
    val intMaxZ = if (maxZ < env.maxZ) maxZ else env.maxZ

    new Envelope(intMinX, intMaxX, intMinY, intMaxY, intMinZ, intMaxZ)
  }


  /**
    * Checks if the region of the input Envelope intersects the region of this Envelope.
    *
    * @param env the Envelope with which the intersection is being checked
    * @return true if the Envelope intersects the other Envelope
    */
  def intersects(env: Envelope): Boolean = {
    if (env.isNull) {
      return false
    }

    !(env.minX > maxX ||
      env.maxX < minX ||
      env.minY > maxY ||
      env.maxY < minY ||
      env.minZ > maxZ ||
      env.maxZ < minZ)
  }

  /**
    * Checks if the region the three external points intersects the region of this Envelope.
    *
    * @param p1 the first external point
    * @param p2 the second external point
    * @param p3 the third external point
    * @return true if the region intersects the other Envelope
    */
  def intersects(p1: Point3D, p2: Point3D, p3: Point3D): Boolean = {
    if (isNull) {
      return false
    }

    val envMinX = min(p1.x, min(p2.x, p3.x))
    if (envMinX > maxX) {
      return false
    }

    val envMaxX = max(p1.x, max(p2.x, p3.x))
    if (envMaxX < minX) {
      return false
    }

    val envMinY = min(p1.y, min(p2.y, p3.y))
    if (envMinY > maxY) {
      return false
    }

    val envMaxY = max(p1.y, max(p2.y, p3.y))
    if (envMaxY < minY) {
      return false
    }

    val envMinZ = min(p1.z, min(p2.z, p3.z))
    if (envMinZ > maxZ) {
      return false
    }

    val envMaxZ = max(p1.z, max(p2.z, p3.z))
    if (envMaxZ < minZ) {
      return false
    }

    true
  }

  /**
    * Check if the point (x, y, z)
    * intersects (lies inside) the region of this Envelope.
    *
    * @param  x the x-coordinate of the point
    * @param  y the y-coordinate of the point
    * @param  z the z-coordinate of the point
    * @return true if the point overlaps this Envelope
    */
  def intersects(x: Double, y: Double, z: Double): Boolean = {
    if (isNull) {
      return false
    }

    !(x < minX ||
      x > maxX ||
      y < minY ||
      y > maxY ||
      z < minZ ||
      z > maxZ)
  }

  /**
    * Tests if the given point lies in or on the envelope.
    *
    * @param p Point3D to be checked for the containment
    * @return true if the p lies in the interior or on the
    *         boundary of this Envelope, false if the Envelope is null.
    */
  def contains(p: Point3D): Boolean = {
    covers(p)
  }

  /**
    * Tests if the Envelope other
    * lies wholely inside this Envelope (inclusive of the boundary).
    *
    * @param  env the Envelope to check
    * @return true if this Envelope covers the other Envelope, false if either of these Envelope is null
    */
  def contains(env: Envelope): Boolean = {
    covers(env)
  }

  /**
    * Tests if the given point lies in or on the envelope.
    *
    * @param  x the x-coordinate of the point which this Envelope is
    *           being checked for containment
    * @param  y the y-coordinate of the point which this Envelope is
    *           being checked for containment
    * @param  z the z-coordinate of the point which this Envelope is
    *           being checked for containment
    * @return true if (x, y, z) lies in the interior or
    *         on the boundary of this Envelope, false if the Envelope is null.
    */
  def contains(x: Double, y: Double, z: Double): Boolean = {
    covers(x, y, z)
  }

  /**
    * Tests if the given point lies in or on the envelope.
    *
    * @param p Point3D to be checked for the containment
    * @return true if the p lies in the interior or on the
    *         boundary of this Envelope, false if the Envelope is null.
    */
  def covers(p: Point3D): Boolean = {
    covers(p.x, p.y, p.z)
  }

  /**
    * Tests if the given point lies in or on the envelope.
    *
    * @param  x the x-coordinate of the point which this Envelope is
    *           being checked for containment
    * @param  y the y-coordinate of the point which this Envelope is
    *           being checked for containment
    * @param  z the z-coordinate of the point which this Envelope is
    *           being checked for containment
    * @return true if (x, y, z) lies in the interior or
    *         on the boundary of this Envelope, false if the Envelope is null.
    */
  def covers(x: Double, y: Double, z: Double): Boolean = {
    if (isNull) {
      return false
    }

    x >= minX &&
      x <= maxX &&
      y >= minY &&
      y <= maxY &&
      z >= minZ &&
      z <= maxZ

  }

  /**
    * Tests if the Envelope other
    * lies wholely inside this Envelope (inclusive of the boundary).
    *
    * @param  env the Envelope to check
    * @return true if this Envelope covers the other Envelope, false if either of these Envelope is null
    */
  def covers(env: Envelope): Boolean = {
    if (isNull || env.isNull) {
      return false
    }

    env.minX >= minX &&
      env.maxX <= maxX &&
      env.minY >= minY &&
      env.maxY <= maxY &&
      env.minZ >= minZ &&
      env.maxZ <= maxZ
  }

  /**
    * Computes the distance between this and another Envelope
    * The distance between overlapping Envelopes is 0.  Otherwise, the
    * distance is the Euclidean distance between the closest points.
    *
    * @param env the other Envelope from which distance is to be computed
    * @return the distance between the two Envelopes
    */
  def distance(env: Envelope): Double = {
    if(intersects(env)) return 0.0

    var dx = 0.0
    if (maxX < env.minX) {
      dx = env.minX - maxX
    } else if (minX > env.maxX) {
      dx = minX - env.maxX
    }

    var dy = 0.0
    if (maxY < env.minY) {
      dy = env.minY - maxY
    } else if (minY > env.maxY) {
      dy = minY - env.maxY
    }

    var dz = 0.0
    if (maxZ < env.minZ) {
      dz = env.minZ - maxZ
    } else if (minZ > env.maxZ) {
      dz = minZ - env.maxZ
    }

    Math.sqrt(dx * dx + dy * dy + dz * dz)
  }

  /**
    * Checks if the input Envelope is equal to the this Envelope. Return false if
    * the input Object is not an instance of the Envelope class or either of these
    * Envelopes is Empty.
    *
    * @param other the other Envelope for which the equality is to be checked
    * @return true if the two Envelopes are equal, false otherwise
    */
  def isEqual(other: AnyRef): Boolean = {
    if (!(other.isInstanceOf[Envelope])) {
      return false
    }

    val env: Envelope = other.asInstanceOf[Envelope]

    if (isNull) {
      return env.isNull
    }

    minX == env.minX &&
      maxX == env.maxX &&
      minY == env.minY &&
      maxY == env.maxY &&
      minZ == env.minZ &&
      maxZ == env.maxZ

  }

  /**
    * Represent the Envelope as a String
    *
    * @return String representation of the Envelope
    */
  override def toString(): String = {
    "Env[" +
      minX + " : " + maxX + ", " +
      minY + " : " + maxY + ", " +
      minZ + " : " + maxZ + ", " +
      "]"
  }
}

object Envelope {

  override def hashCode(): Int = {
    // to do - implement hashing function
    0
  }

  /**
    * Tests if the point q intersects the Envelope defined by the coordinates p1, p2 and p3, Envelope p1-p2-p3
    *
    * @param q point to test intersection for
    * @param p1 one external point to the Envelope
    * @param p2 second external point of the Envelope
    * @param p3 third external point of the Envelope
    * @return returns true if the point intersects Envelope p1-p2
    */
  def intersects(q: Point3D, p1: Point3D, p2: Point3D, p3: Point3D): Boolean = {
    val minX = min(p1.x, min(p2.x, p3.x))
    val minY = min(p1.y, min(p2.y, p3.y))
    val minZ = min(p1.z, min(p2.z, p3.z))
    val maxX = max(p1.x, max(p2.x, p3.x))
    val maxY = max(p1.y, max(p2.y, p3.y))
    val maxZ = max(p1.z, max(p2.z, p3.z))

    if ((q.x >= minX) && (q.x <= maxX) &&
      (q.y >= minY) && (q.y <= maxY) &&
      (q.z >= minZ) && (q.z <= maxZ)) {
      return true
    }
    false
  }

  /**
    * Tests if the Envelope defined by the coordinates p1, p2 and p3 (Enveleop p1-p2-p3) intersects the Envelope defined
    * by the coordinates q1, q2 and q3 (Envelope q1-q2-q3)
    *
    * @param p1 one external point to the Envelope q1-q2-q3
    * @param p2 one external point to the Envelope q1-q2-q3
    * @param q1 one external point to the Envelope p1-p2-p3
    * @param q2 one external point to the Envelope p1-p2-p3
    * @return returns true if the Envelope p1-p2-p3 intersects Envelope q1-q2-q3
    */
  def intersects(p1: Point3D, p2: Point3D, p3: Point3D, q1: Point3D, q2: Point3D, q3: Point3D): Boolean = {
    val minpX = min(p1.x, min(p2.x, p3.x))
    val maxpX = max(p1.x, max(p2.x, p3.x))
    val minqX = min(q1.x, min(q2.x, q3.x))
    val maxqX = max(q1.x, max(q2.x, q3.x))

    if ((minpX > maxqX) || (maxpX < minqX)) {
      return false
    }

    val minpY = min(p1.y, min(p2.y, p3.y))
    val maxpY = max(p1.y, max(p2.y, p3.y))
    val minqY = min(q1.y, min(q2.y, q3.y))
    val maxqY = max(q1.y, max(q2.y, q3.y))

    if ((minpY > maxqY) || (maxpY < minqY)) {
      return false
    }

    val minpZ = min(p1.z, min(p2.z, p3.z))
    val maxpZ = max(p1.z, max(p2.z, p3.z))
    val minqZ = min(q1.z, min(q2.z, q3.z))
    val maxqZ = max(q1.z, max(q2.z, q3.z))

    if ((minpZ > maxqZ) || (maxpZ < minqZ)) {
      return false
    }

    true
  }
}
