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
package com.astrolabsoftware.spark3d.utils

import com.astrolabsoftware.spark3d.utils._

import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Test class for the Sphere class.
  */
class GridTypeTest extends FunSuite with BeforeAndAfterAll {

  // Completely dumb test... Must change that...
  test("Do you know the names of the implemented grids?") {
    assert(GridType.LINEARONIONGRID == "LINEARONIONGRID")
    assert(GridType.OCTREE == "OCTREE")
  }
}
