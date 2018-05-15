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

/**
  * Test class for the Point3DTest class.
  */
class SphereTest extends FunSuite with BeforeAndAfterAll {

  // Just test the Sphere class constructor.
  test("Can you initialise a Sphere?") {
    val sphere = new Sphere(0.0, 0.0, 0.0, 1.0)
    assert(sphere.isInstanceOf[Sphere])
  }

  // Are two spheres overlapping?
  test("Can you detect that 2 spheres are overlapping?") {
    val sphere1 = new Sphere(0.0, 0.0, 0.0, 1.0)
    val sphere2 = new Sphere(1.0, 1.0, 1.0, 1.0)
    assert(sphere1.intersect(sphere2))
  }

  // Are two spheres overlapping?
  test("Can you detect that 2 spheres are not overlapping?") {
    val sphere1 = new Sphere(0.0, 0.0, 0.0, 0.5)
    val sphere2 = new Sphere(5.0, 5.0, 5.0, 0.5)
    assert(!sphere1.intersect(sphere2))
  }

  // Are a sphere and a point overlapping?
  test("Can you detect that a point lies inside a sphere?") {
    val sphere = new Sphere(0.0, 0.0, 0.0, 1.0)
    val point = new Point3D(0.0, 0.5, 0.0)
    assert(sphere.intersect(point) && point.intersect(sphere))
  }

  // Volume of a sphere
  test("Can you compute the volume of a sphere?") {
    val sphere = new Sphere(0.0, 0.0, 0.0, 3.0)
    assert(sphere.getVolume == 36 * math.Pi)
  }
}
