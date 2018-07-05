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
package com.astrolabsoftware.spark3d.geometryObjects

import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._
import com.astrolabsoftware.spark3d.utils.Utils.sphericalToCartesian

import scala.math._

/** Defines a cubical region of 3D coordinate space.
  * This can be used to define a bounding box of a geometryObject
  *
  * An cube Envelope can be uniquely defined based on minimum and maximum coordinates along all
  * three axes. On creating the cube Envelope initially, the min's and max's are assigned automatically.
  *
  * Default constructor is kept private to avoid creating an instance of the cube Envelope class without initialising
  * the min/max coordinates along axes incorrectly.
  *
  * @param minX minimum coordinate of cube Envelope along X-axis
  * @param maxX maximum coordinate of cube Envelope along X-axis
  * @param minY minimum coordinate of cube Envelope along Y-axis
  * @param maxY maximum coordinate of cube Envelope along Y-axis
  * @param minZ minimum coordinate of cube Envelope along Z-axis
  * @param maxZ maximum coordinate of cube Envelope along Z-axis
  */
class BoxEnvelope private(
    var minX: Double, var maxX: Double,
    var minY: Double, var maxY: Double,
    var minZ: Double, var maxZ: Double)
  extends Shape3D with Serializable {

  /**
    * Attach an id to the BoxEnvelope to be used while assigning partition ID.
    */
  var indexID: Int = _

  /**
    * Creates a null cube Envelope
    */
  def this() {
    this(0.0, -1.0, 0.0, -1.0, 0.0, -1.0)
  }

  /**
    * Creates an cube Envelope for a region defined by three coordinates.
    * For the moment, coordinates must be cartesian. Error is thrown otherwise.
    *
    * @param p1 first coordinate (cartesian coordinate)
    * @param p2 second coordinate (cartesian coordinate)
    * @param p3 third coordinate (cartesian coordinate)
    */
  def this(p1: Point3D, p2: Point3D, p3: Point3D) {
    this(
      min(p1.x, min(p2.x, p3.x)), max(p1.x, max(p2.x, p3.x)),
      min(p1.y, min(p2.y, p3.y)), max(p1.y, max(p2.y, p3.y)),
      min(p1.z, min(p2.z, p3.z)), max(p1.z, max(p2.z, p3.z))
    )

    // Trigger an AssertionError if Point3D is spherical.
    // Unfortunately, one cannot put the logic first in an
    // overloaded constructor in scala. Another option would
    // be to use apply(...) in the object.
    if (p1.isSpherical | p2.isSpherical | p3.isSpherical) {
      throw new AssertionError("""
        All input Point3D for creating a BoxEnvelope must have cartesian coordinate system!
        Check it using p.isSpherical, and convert them
        using Utils.euclideantoSpherical(p).
        """)
    }
  }

  /**
    * Creates an cube Envelope for a region defined by two coordinates.
    * For the moment, coordinates must be cartesian. Error is thrown otherwise.
    *
    * @param p1 first coordinate (cartesian coordinate)
    * @param p2 second coordinate (cartesian coordinate)
    */
  def this(p1: Point3D, p2: Point3D) {
    this(
      min(p1.x, p2.x), max(p1.x, p2.x),
      min(p1.y, p2.y), max(p1.y, p2.y),
      min(p1.z, p2.z), max(p1.z, p2.z)
    )

    // Trigger an AssertionError if Point3D is spherical.
    // Unfortunately, one cannot put the logic first in an
    // overloaded constructor in scala. Another option would
    // be to use apply(...) in the object.
    if (p1.isSpherical | p2.isSpherical) {
      throw new AssertionError("""
        All input Point3D for creating a BoxEnvelope must have cartesian coordinate system!
        Check it using p.isSpherical, and convert them
        using Utils.euclideantoSpherical(p).
        """)
    }
  }

  /**
    * Creates an cube Envelope for a region defined one coordinate.
    * The cube Envelope in this case will be a point.
    * For the moment, coordinates must be cartesian. Error is thrown otherwise.
    *
    * @param p1 the coordinate (cartesian coordinate)
    */
  def this(p1: Point3D) {
    this(p1.x, p1.x, p1.y, p1.y, p1.z, p1.z)

    // Trigger an AssertionError if Point3D is spherical.
    // Unfortunately, one cannot put the logic first in an
    // overloaded constructor in scala. Another option would
    // be to use apply(...) in the object.
    if (p1.isSpherical) {
      throw new AssertionError("""
        Input Point3D for creating a BoxEnvelope must have cartesian coordinate system!
        Check it using p.isSpherical, and convert them
        using Utils.euclideantoSpherical(p).
        """)
    }
  }

  /**
    * Clones an existing cube Envelope to create a duplicate cube Envelope.
    *
    * @param env original cube Envelope to be cloned
    */
  def this(env: BoxEnvelope) {
    this(env.minX, env.maxX, env.minY, env.maxY, env.minZ, env.maxZ)
  }

  /**
    * Checks if this is a null envelope or not.
    *
    * @return if this cube Envelope is null (empty Geometry) or nots
    */
  def isNull(): Boolean = {
    ((minX > maxX) || (minY > maxY) || (minZ > maxZ))
  }

  /**
    * Sets this cube Envelope to null
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
    * Return the difference between max and min X values of the cube Envelope.
    *
    * @return maxX - minX, or 0 if the cube Envelope is null
    */
  def getXLength(): Double = {

    if (isNull) {
      return 0.0
    }
    maxX - minX
  }

  /**
    * Returns the difference between max and min Y value of the cube Envelope.
    *
    * @return maxX - minY, or 0 if the cube Envelope is null
    */
  def getYLength(): Double = {

    if (isNull) {
      return 0.0
    }
    maxY - minY
  }

  /**
    * Return the difference between max and min Z value of the cube Envelope.
    *
    * @return maxZ - minZ, or 0 if the cube Envelope is null
    */
  def getZLength(): Double = {

    if (isNull) {
      return 0.0
    }
    maxZ - minZ
  }

  /**
    * Gets minimum extent of this cube Envelope across all three dimensions.
    *
    * @return the minimum extent of this cube Envelope
    */
  def minExtent(): Double = {
    if (isNull) {
      return 0.0
    }

    min(getXLength(), min(getYLength(), getZLength()))
  }

  /**
    * Gets maximum extent of this cube Envelope across all three dimensions.
    *
    * @return the maximum extent of this cube Envelope
    */
  def maxExtent(): Double = {
    if (isNull) {
      return 0.0
    }

    max(getXLength(), max(getYLength(), getZLength()))
  }

  /**
    * Returns the volume of the cube Envelope.
    *
    * @return the volume of the envelope, 0.0 if the cube Envelope is null
    */
  def getVolume(): Double = {
    getXLength() * getYLength() * getZLength()
  }


  /**
    * Expand the cube Envelope so that it contains the given Point
    * For the moment, coordinates must be cartesian. Error is thrown otherwise.
    *
    * @param p the Point to expand to include (cartesian coordinate).
    */
  def expandToInclude(p: Point3D): Unit = {
    if (p.isSpherical) {
      throw new AssertionError("""
        Input Point3D for expanding a BoxEnvelope must have cartesian coordinate system!
        Check it using p.isSpherical, and convert it
        using Utils.euclideantoSpherical(p).
        """)
    }
    expandToInclude(p.x, p.y, p.z)
  }

  /**
    * Expand cube Envelope by given distance along the all three dimensions.
    *
    * @param delta the distance to expand the cube Envelope along all the axes
    */
  def expandBy(delta: Double): Unit = {
    expandBy(delta, delta, delta)
  }

  /**
    * Expand cube Envelope by given distances along the three dimension.
    *
    * @param deltaX the distance to expand the cube Envelope along the the X axis
    * @param deltaY the distance to expand the cube Envelope along the the Y axis
    * @param deltaZ the distance to expand the cube Envelope along the the Z axis
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

  }

  /**
    * Enlarges this cube Envelope so that it contains the given point.
    * Has no effect if the point is already on or within the envelope.
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
    * Expand the cube Envelope so that it includes the other cube Envelope.
    *
    * @param env the cube Envelope to expand to include
    */
  def expandToInclude(env: BoxEnvelope): Unit = {

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

  override val center = new Point3D(
    (minX + maxX) / 2.0,
    (minY + maxY) / 2.0,
    (minZ + maxZ) / 2.0,
    false)

  /**
    * Comptutes the intersection of the two cube Envelopes
    *
    * @param env the envelope to find intersection with
    * @return a new cube Envelope representing the intersection of the envelopes (this will be
    * the null envelope if either if the envelopes is null, or they do not intersect
    */
  def intersection(env: BoxEnvelope): BoxEnvelope = {
    if (isNull || env.isNull) {
      return null
    }

    val intMinX = if (minX < env.minX) minX else env.minX
    val intMaxX = if (maxX > env.maxX) maxX else env.maxX
    val intMinY = if (minY < env.minY) minY else env.minY
    val intMaxY = if (maxY > env.maxY) maxY else env.maxY
    val intMinZ = if (minZ < env.minZ) minZ else env.minZ
    val intMaxZ = if (maxZ > env.maxZ) maxZ else env.maxZ

    new BoxEnvelope(intMinX, intMaxX, intMinY, intMaxY, intMinZ, intMaxZ)
  }

  /**
    * Methods to determine whether the Box intersects another shape.
    * Implement different ways for different shapes (Point, Shell, Box available).
    *
    * @param otherShape : (Shape3D)
    *   An instance of Shape3D (or extension)
    * @return (Boolean) true if the two objects intersect.
    *
    */
  override def intersects(otherShape: Shape3D): Boolean = {

    // Different methods to handle different shapes
    if (otherShape.isInstanceOf[Point3D]) {
      this.covers(otherShape.asInstanceOf[Point3D])
    } else if (otherShape.isInstanceOf[ShellEnvelope]) {
      // This is not perfect. We take the bounding box around the shell
      // and perform the intersection between boxes.
      // Potential bugs:
      //  - if the box is within the inner shell
      //  - if the box is just outside the outer shell
      // TODO: Implement real shell - box intersection.
      val env = otherShape.asInstanceOf[ShellEnvelope].getEnvelope
      this.intersectsBox(env)
    } else if (otherShape.isInstanceOf[BoxEnvelope]) {
      this.intersectsBox(otherShape.asInstanceOf[BoxEnvelope])
    } else {
      throw new AssertionError(
        """
        Cannot perform intersection because the type of shape is unknown!
        Currently implemented:
          - box x point
          - box x sphere
          - box x box
        """)
    }
  }


  /**
    * Checks if the region of the input cube Envelope intersects the region of this cube Envelope.
    *
    * @param env the cube Envelope with which the intersection is being checked
    * @return true if the cube Envelope intersects the other cube Envelope
    */
  def intersectsBox(env: BoxEnvelope): Boolean = {
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
    * Checks if the region the three external points
    * intersects the region of this cube Envelope.
    * For the moment, coordinates must be cartesian. Error is thrown otherwise.
    *
    * @param p1 the first external point (cartesian coordinate)
    * @param p2 the second external point (cartesian coordinate)
    * @param p3 the third external point (cartesian coordinate)
    * @return true if the region intersects the other cube Envelope
    */
  def intersectsRegion(p1: Point3D, p2: Point3D, p3: Point3D): Boolean = {
    if (p1.isSpherical | p2.isSpherical | p3.isSpherical) {
      throw new AssertionError("""
        All input Point3D for creating a region must have cartesian coordinate system!
        Check it using p.isSpherical, or convert them
        using Utils.euclideantoSpherical(p).
        """)
    }
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
    * intersects (lies inside) the region of this cube Envelope.
    *
    * @param  x the x-coordinate of the point
    * @param  y the y-coordinate of the point
    * @param  z the z-coordinate of the point
    * @return true if the point overlaps this cube Envelope
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
    * For the moment, coordinates must be cartesian. Error is thrown otherwise.
    *
    * @param p Point3D to be checked for the containment (cartesian coordinate)
    * @return true if the p lies in the interior or on the
    *         boundary of this cube Envelope, false if the cube Envelope is null.
    */
  def contains(p: Point3D): Boolean = {
    if (p.isSpherical) {
      throw new AssertionError("""
        Input Point3D must have cartesian coordinate system!
        Check it using p.isSpherical, and convert it
        using Utils.euclideantoSpherical(p).
        """)
    }
    covers(p)
  }

  /**
    * Tests if the cube Envelope other
    * lies wholely inside this cube Envelope (inclusive of the boundary).
    *
    * @param  env the cube Envelope to check
    * @return true if this cube Envelope covers the other cube Envelope, false if either of these cube Envelope is null
    */
  def contains(env: BoxEnvelope): Boolean = {
    covers(env)
  }

  /**
    * Tests if the given point lies in or on the envelope.
    *
    * @param  x the x-coordinate of the point for which the containment is to be checked
    * @param  y the y-coordinate of the point for which the containment is to be checked
    * @param  z the z-coordinate of the point for which the containment is to be checked
    * @return true if (x, y, z) lies in the interior or
    *         on the boundary of this cube Envelope, false if the cube Envelope is null.
    */
  def contains(x: Double, y: Double, z: Double): Boolean = {
    covers(x, y, z)
  }

  /**
    * Tests if the given point lies in or on the envelope.
    * For the moment, coordinates must be cartesian. Error is thrown otherwise.
    *
    * @param p Point3D to be checked for the containment (cartesian coordinate)
    * @return true if the p lies in the interior or on the
    *         boundary of this cube Envelope, false if the cube Envelope is null.
    */
  def covers(p: Point3D): Boolean = {
    if (p.isSpherical) {
      throw new AssertionError("""
        Input Point3D must have cartesian coordinate system!
        Check it using p.isSpherical, and convert it
        using Utils.euclideantoSpherical(p).
        """)
    }
    covers(p.x, p.y, p.z)
  }

  /**
    * Tests if the given point lies in or on the envelope.
    *
    * @param  x the x-coordinate of the point for which this cube Envelope is
    *           being checked for containment
    * @param  y the y-coordinate of the point for which this cube Envelope is
    *           being checked for containment
    * @param  z the z-coordinate of the point for which this cube Envelope is
    *           being checked for containment
    * @return true if (x, y, z) lies in the interior or
    *         on the boundary of this cube Envelope, false if the cube Envelope is null.
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
    * Tests if the cube Envelope other
    * lies completely inside this cube Envelope (inclusive of the boundary).
    *
    * @param  env the cube Envelope to check
    * @return true if this cube Envelope covers the other cube Envelope, false if either of these cube Envelope is null
    */
  def covers(env: BoxEnvelope): Boolean = {
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
    * Computes the distance between this and another cube Envelope
    * The distance between overlapping cube Envelopes is 0.  Otherwise, the
    * distance is the Euclidean distance between the closest points.
    *
    * @param env the other cube Envelope from which distance is to be computed
    * @return the distance between the two cube Envelopes
    */
  def distance(env: BoxEnvelope): Double = {
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
    * Checks if the input cube Envelope is equal to the this cube Envelope. Return false if
    * the input Object is not an instance of the cube Envelope class or either of these
    * cube Envelopes is Empty.
    *
    * @param other the other cube Envelope for which the equality is to be checked
    * @return true if the two cube Envelopes are equal, false otherwise
    */
  def isEqual(other: AnyRef): Boolean = {
    if (!(other.isInstanceOf[BoxEnvelope])) {
      return false
    }

    val env: BoxEnvelope = other.asInstanceOf[BoxEnvelope]

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
    * Represent the cube Envelope as a String
    *
    * @return String representation of the cube Envelope
    */
  override def toString(): String = {
    "Env[" +
      minX + " : " + maxX + ", " +
      minY + " : " + maxY + ", " +
      minZ + " : " + maxZ + ", " +
      "]"
  }

  /**
    * The box envelope is already an envelope!
    * @return (BoxEnvelope) the BoxEnvelope instance.
    */
  override def getEnvelope: BoxEnvelope = this
}

object BoxEnvelope {

  def apply(
      x1: Double, x2: Double,
      y1: Double, y2: Double,
      z1: Double, z2: Double): BoxEnvelope = {
    new BoxEnvelope(
      min(x1, x2), max(x1, x2),
      min(y1, y2), max(y1, y2),
      min(z1, z2), max(z1, z2)
    )
  }
}
