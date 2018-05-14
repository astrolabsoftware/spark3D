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

/**
  * Test class for the Point3DTest class.
  */
class Point3DTest extends FunSuite with BeforeAndAfterAll {

  // Just test the Point3D class constructor.
  test("Can you initialise a Point3D?") {
    val p = new Point3D(0.0, 0.0, 0.0)
    assert(p.isInstanceOf[Point3D])
  }

  // Test method to compute the distance between 2 points.
  test("Can you compute the distance between 2 points?") {
    val p1 = new Point3D(0.0, 0.0, 0.0)
    val p2 = new Point3D(1.0, 1.0, 1.0)
    assert(p1.distanceTo(p2) == math.sqrt(3))
  }
}
