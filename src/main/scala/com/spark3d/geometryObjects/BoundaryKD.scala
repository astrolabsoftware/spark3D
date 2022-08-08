/*
 * Copyright 2018 AstroLab Software
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
 * limitations under the License.env
 */
package com.astrolabsoftware.spark3d.geometryObjects

import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._

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
class BoundaryKD private(
    var minX: Double, var maxX: Double,
    var minY: Double, var maxY: Double,
    var minZ: Double, var maxZ: Double)
  extends  Serializable {

    

  }
