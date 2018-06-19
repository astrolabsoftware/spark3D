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
package com.spark3d.spatialOperator

import scala.reflect.ClassTag

import com.spark3d.geometryObjects.Shape3D._

import com.spark3d.spatial3DRDD.Shape3DRDD

import org.apache.spark.rdd.RDD

object RangeQuery {

  /**
    *
    */
  def windowQuery[A<:Shape3D : ClassTag, B<:Shape3D : ClassTag](
    rdd: RDD[A], enveloppeWindow: B): RDD[A] = {
      // Just intersection -- need to implement full coverage
      rdd.filter(element => element.intersects(enveloppeWindow))
    }
  // /**
  //   *
  //   */
  // def windowQuery[A<:Shape3D : ClassTag, B<:Shape3D : ClassTag](
  //   element: A, enveloppeWindow: B): Boolean = {
  //     // Just intersection -- need to implement full coverage
  //     element.intersects(enveloppeWindow)
  //   }
}
