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
package com.astrolabsoftware.spark3d.geometryObjects

import com.astrolabsoftware.spark3d.utils.Utils._
import com.astrolabsoftware.spark3d.utils.ExtPointing

import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.RING

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
      * Get the bounding box of the Shape3D
      * @return bounding box (Cuboid) of the Shape3D
      */
    def getEnvelope: BoxEnvelope

    /**
      * Methods to determine whether the Shape3D is contained in another Shape3D.
      * Implement different ways for different shapes (Point, Shell, Box available).
      *
      * @param otherShape : (Shape3D)
      *   An instance of Shape3D (or extension)
      * @return (Boolean) true if the two objects intersect.
      *
      */
    def intersects(otherShape: Shape3D): Boolean

    /**
      * Compute the healpix index of the geometry center.
      * By default, the method considers that this.y = ra, this.z = dec.
      * You can also bypass that, and force this.y = theta, this.z = phi by
      * setting thetaphi = true.
      * We only consider the RING scheme for the moment.
      *
      * @param nside : (Int)
      *   Resolution of the healpix map.
      * @param thetaphi : (Boolean)
      *   Convention for your data: this.y = ra, this.z = dec if false,
      *   this.y = theta, this.z = phi otherwise. Default is false.
      * @return (Long) Healpix index of the point for the resolution chosen.
      *
      */
    def toHealpix(nside: Int, thetaphi: Boolean = false): Long = {
      assert(center.isSpherical)

      // Initialise the Pointing object
      var ptg = new ExtPointing

      // Initialise HealpixBase functionalities
      val hp = new HealpixBase(nside, RING)

      // Make coordinate conversion if needed
      ptg.theta = if (!thetaphi) {
        dec2theta(center.z)
      } else center.y

      ptg.phi = if (!thetaphi) {
        ra2phi(center.y)
      } else center.z

      // Compute the index
      hp.ang2pix(ptg)
    }

    /**
      * Return if the input Point3D is equal this Point3D
      *
      * @param p Point3D for which the comparison has to be done
      * @return true if the two Point3D centers are within epsilon
      */
    def hasCenterCloseTo(p: Point3D, epsilon: Double): Boolean = {
      center.distanceTo(p) <= epsilon
    }

    def getHash(): Int = {
      (center.getCoordinate.mkString("/") + getEnvelope.toString).hashCode
    }
  }
}
