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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.spark3d.geometryObjects._
import com.spark3d.geometryObjects.Shape3D._

/**
  * Test class for the Shape3D methods.
  */
class Shape3DTest extends FunSuite with BeforeAndAfterAll {
  test("Can you catch bad intersections between a spherical and a non-spherical object?") {
    val p = new Point3D(0.0, 0.0, 0.0, true)
    val wrong = new nonShape
    val exception1 = intercept[AssertionError] {
      sphereSphereIntersection(p, wrong)
    }
    assert(exception1.getMessage.contains("non-spherical object"))

    val exception2 = intercept[AssertionError] {
      sphereSphereIntersection(wrong, p)
    }
    assert(exception2.getMessage.contains("non-spherical object"))
  }

  test("Can you detect that a point belong to a shell?") {
    val p = new Point3D(1.0, 1.0, 1.0, false)
    val lower_sphere = new Sphere(0.0, 0.0, 0.0, 1.0)
    val upper_sphere = new Sphere(0.0, 0.0, 0.0, 5.0)
    assert(isPointInShell(lower_sphere, upper_sphere, p))
  }

  test("Can you detect that a point outside a shell?") {
    val p = new Point3D(10.0, 10.0, 10.0, false)
    val lower_sphere = new Sphere(0.0, 0.0, 0.0, 1.0)
    val upper_sphere = new Sphere(0.0, 0.0, 0.0, 5.0)
    assert(!isPointInShell(lower_sphere, upper_sphere, p))
  }

  test("Can you detect a bad shell (non-centered spheres)?") {
    val p = new Point3D(1.0, 1.0, 1.0, false)
    val lower_sphere = new Sphere(1.0, 0.0, 0.0, 1.0)
    val upper_sphere = new Sphere(0.0, 0.0, 0.0, 5.0)
    val exception = intercept[AssertionError] {
      isPointInShell(lower_sphere, upper_sphere, p)
    }
    assert(exception.getMessage.contains("must be centered"))
  }

  test("Can you detect two nearby points?") {
    val p1 = new Point3D(1.0, 1.0, 1.0, false)
    val p2 = new Point3D(2.0, 1.0, 1.0, false)
    val p3 = new Point3D(20.0, 10.0, 10.0, false)
    val epsilon = 5.0
    assert(p1.hasCenterCloseTo(p2, epsilon) && !p1.hasCenterCloseTo(p3, epsilon))
  }
}
