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

/**
  * Define a new enumeration with a type alias
  * and work with the full set of enumerated values.
  * The Java equivalent is enum.
  *
  */
object GridType extends Enumeration {

  type GridType = Value

  // Add new GridType name here once implementation done.
  // Names should be comma separated: val ONIONGRID, TOTO = Value
  val LINEARONIONGRID, OCTREE = Value
}
