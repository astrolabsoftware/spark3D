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
package com.spark3d

object Utils {

  /**
    * Convert declination into theta
    *
    * @param dec : (Double)
    *   declination coordinate in degree
    * @return theta coordinate in radian
    *
    */
  def dec2theta(dec : Double) : Double = {
    Math.PI / 2.0 - Math.PI / 180.0 * dec
  }

  /**
    * Convert right ascension into phi
    *
    * @param ra : (Double)
    *   RA coordinate in degree
    * @return phi coordinate in radian
    *
    */
  def ra2phi(ra : Double) : Double = {
    Math.PI / 180.0 * ra
  }
}
