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

import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._
import com.astrolabsoftware.spark3d.utils.Utils.sphericalToCartesian

/**
  * Dummy class with no specific implementation to test errors
  */
class nonShape extends Shape3D {
  // Centered in 0
  val center : Point3D = new Point3D(0.0, 0.0, 0.0, true)

  def getEnvelope: BoxEnvelope = ???

  def intersects(otherShape: Shape3D): Boolean = ???
}

/**
  * Test class for the Point3DTest class.
  */
class Point3DTest extends FunSuite with BeforeAndAfterAll {

  // Just test the Point3D class constructor.
  test("Can you initialise a Point3D?") {
    val p = new Point3D(0.0, 0.0, 0.0, true)
    assert(p.isInstanceOf[Point3D])
  }

  // Test method to compute the distance between 2 points.
  test("Can you compute the distance between 2 points (euclidean)?") {
    val p1 = new Point3D(0.0, 0.0, 0.0, false)
    val p2 = new Point3D(1.0, 1.0, 1.0, false)
    assert(p1.distanceTo(p2) == math.sqrt(3))
  }

  // Test method to compute the distance between 2 points.
  test("Can you compute the distance between 2 points (spherical)?") {
    val p1 = new Point3D(0.0, 0.0, 0.0, true)
    val p2 = new Point3D(1.0, math.Pi/2.0, math.Pi, true)
    assert(p1.distanceTo(p2) == 1.0)
  }

  test("Return correctly the coordinates of a point?") {
    val p1 = new Point3D(0.0, 1.0, 0.0, true)
    assert(p1.getCoordinate == List(0.0, 1.0, 0.0))
  }

  // Python interface
  test("Return correctly the coordinates of a point (Python interface)?") {
    val p1 = new Point3D(0.0, 1.0, 0.0, true)
    assert(p1.getCoordinatePython == List(0.0, 1.0, 0.0).asJava)
  }

  // Test method to test whether two points intersect
  test("Can you identify two different points?") {
    val p1 = new Point3D(0.0, 1.0, 0.0, false)
    val p2 = new Point3D(0.1, 1.0, 0.0, false)
    assert(!p1.intersects(p2))
  }

  // Test method to test whether two points intersect
  test("Can you identify two identical points?") {
    val p1 = new Point3D(0.0, 1.0, 0.0, false)
    val p2 = new Point3D(0.0, 1.0, 0.0, false)
    assert(p1.intersects(p2))
  }

  test("Can you intersect a point and a Shell?") {
    val p1 = new Point3D(0.5, 0.0, 0.0, true)
    val shell = new ShellEnvelope(0.0, 0.0, 0.0, true, 0.0, 10.0)
    assert(p1.intersects(shell))

    val p2 = new Point3D(0.5, 0.0, 0.0, false)
    val exception = intercept[AssertionError] {
      p2.intersects(shell)
    }
    assert(exception.getMessage.contains("The 2 points must have the same coordinate system"))
  }

  test("Can you intersect a point and a Box?") {
    val p1 = new Point3D(0.0, 1.0, 0.0, false)
    val p2 = new Point3D(10.0, 0.0, 0.0, false)
    val p3 = new Point3D(0.0, 10.0, 0.0, false)
    val p4 = new Point3D(0.0, 0.0, 10.0, false)
    val box = new BoxEnvelope(p2, p3, p4)
    assert(p1.intersects(box))
  }

  test("Can you catch an error trying to intersect a point with spherical coordinate and a Box?") {
    val p1 = new Point3D(0.0, 1.0, 0.0, true)
    val p2 = new Point3D(10.0, 0.0, 0.0, false)
    val p3 = new Point3D(0.0, 10.0, 0.0, false)
    val p4 = new Point3D(0.0, 0.0, 10.0, false)
    val box = new BoxEnvelope(p2, p3, p4)

    val exception = intercept[AssertionError] {
      p1.intersects(box)
    }
    assert(exception.getMessage.contains("must have cartesian coordinate system"))

    val exception2 = intercept[AssertionError] {
      box.covers(p1)
    }
    assert(exception2.getMessage.contains("must have cartesian coordinate system"))
  }

  test("Can you return the envelope around the point (which is the point itself)?") {
    val p = new Point3D(0.0, 1.0, 0.0, false)
    val box = new BoxEnvelope(p)
    assert(p.getEnvelope.isEqual(box))

    val pSph = new Point3D(0.0, 1.0, 0.0, true)
    val boxSph = new BoxEnvelope(sphericalToCartesian(pSph))
    assert(pSph.getEnvelope.isEqual(boxSph))

    // Perform a conversion spherical to cartesian
    // as BoxEnvelope needs cartesian
    val p2 = new Point3D(1.0, 0.0, 0.0, true)

    val exception = intercept[AssertionError] {
      val box2 = new BoxEnvelope(p2)
    }
    assert(exception.getMessage.contains("must have cartesian coordinate system"))
  }

  // Volume of a point
  test("Can you compute the volume of a point?") {
    val p = new Point3D(0.0, 0.0, 0.0, true)
    assert(p.getVolume == 0.0)
  }

  // Detect problems with unknown shape when doing intersection
  test("Can you deal with unknown shape when doing intersection?") {
    val p = new Point3D(0.0, 0.0, 0.0, true)
    val wrong = new nonShape
    val exception = intercept[AssertionError] {
      p.intersects(wrong)
    }
    assert(exception.getMessage.contains("Cannot perform intersection"))
  }

  test("Can you compute the Healpix index from the RA/Dec of a point?") {
    val p = new Point3D(0.0, 1.0, 0.0, true)
    val hpIndex = p.toHealpix(512)

    assert(hpIndex == 1571845)
  }

  test("Can you compute the Healpix index from the theta/phi of a point?") {
    val p = new Point3D(0.0, 0.0, 0.0, true)
    val hpIndex = p.toHealpix(512, true)

    assert(hpIndex == 0)
  }
}
