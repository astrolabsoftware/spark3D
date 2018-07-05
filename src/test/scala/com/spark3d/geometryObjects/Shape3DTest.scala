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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._

/**
  * Test class for the Shape3D methods.
  */
class Shape3DTest extends FunSuite with BeforeAndAfterAll {

  test("Can you detect two nearby points?") {
    val p1 = new Point3D(1.0, 1.0, 1.0, false)
    val p2 = new Point3D(2.0, 1.0, 1.0, false)
    val p3 = new Point3D(20.0, 10.0, 10.0, false)
    val epsilon = 5.0
    assert(p1.hasCenterCloseTo(p2, epsilon) && !p1.hasCenterCloseTo(p3, epsilon))
  }
}
