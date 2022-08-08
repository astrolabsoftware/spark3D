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
 * limitations under the License.
 */
package com.astrolabsoftware.spark3d.utils

/**
  * Register available GridType names used in Shape3DRDD
  * to perform a RDD repartitioning.
  */
object GridType {

  // Add new GridType name here once implementation done.
  val LINEARONIONGRID = "onion"
  val OCTREE = "octree"
  val KDTREE = "kdtree"
}
