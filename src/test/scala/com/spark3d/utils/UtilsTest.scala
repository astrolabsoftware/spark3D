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

import com.spark3d.utils.Utils._
import com.spark3d.geometryObjects.Point3D

import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Test class for the Utils class.
  */
class UtilsTest extends FunSuite with BeforeAndAfterAll {

  // Cartesian -> Spherical -> Cartesian
  test("Can you convert a Point3D with cartesian coordinate into spherical coordinate, and vice versa?") {
    val p_euc = new Point3D(1.0, 1.0, 0.0, false)
    val p_sph = cartesiantoSpherical(p_euc)
    val p_sph_euc = sphericalToCartesian(p_sph)
    assert(
      math.rint(p_euc.x) == math.rint(p_sph_euc.x) &&
      math.rint(p_euc.y) == math.rint(p_sph_euc.y) &&
      math.rint(p_euc.z) == math.rint(p_sph_euc.z))
  }

  // Detect problems with unknown shape when doing intersection
  test("Can you catch a bad conversion cartesian <-> spherical?") {
    // Cartesian
    val p_euc = new Point3D(0.0, 0.0, 0.0, false)
    val exception1 = intercept[AssertionError] {
      sphericalToCartesian(p_euc)
    }
    assert(exception1.getMessage.contains("already in cartesian coordinates"))

    // Spherical
    val p_sph = new Point3D(0.0, 0.0, 0.0, true)
    val exception2 = intercept[AssertionError] {
      cartesiantoSpherical(p_sph)
    }
    assert(exception2.getMessage.contains("already in spherical coordinates"))
  }

  // deg to rad conversion
  test("Can you convert RA/Dec (deg) to theta/phi (rad) correctly?") {
    val ra = 180.0
    val dec = 0.0

    assert(dec2theta(dec) == math.Pi/2.0 && ra2phi(ra) == math.Pi)
  }

  // rad to rad conversion
  test("Can you convert RA/Dec (rad) to theta/phi (rad) correctly?") {
    val ra = math.Pi
    val dec = 0.0

    assert(dec2theta(dec, true) == math.Pi/2.0 && ra2phi(ra, true) == math.Pi)
  }
}
