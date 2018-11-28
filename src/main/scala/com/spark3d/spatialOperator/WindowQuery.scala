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
package com.astrolabsoftware.spark3d.spatialOperator

import scala.reflect.ClassTag

import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._

import org.apache.spark.rdd.RDD

/**
  * Handle window query.
  * Note that window query is just a sub-case of CrossMatch, just one of the
  * two data sets has only one (potentially extended) element... ;-)
  */
object WindowQuery {

  /**
    * Perform window query, that is match between RDD elements and
    * a user-defined window (point, shell, box).
    *
    * @param rdd: (RDD[A<:Shape3D])
    *   RDD of 3D objects
    * @param envelopeWindow : (B<:Shape3D)
    *   3D envelope inside which the query is performed
    * @return (RDD[A<:Shape3D]) RDD with only elements of rdd inside the
    *   envelopeWindow
    *
    */
  def windowQueryFromRDD[A<:Shape3D : ClassTag, B<:Shape3D : ClassTag](
    rdd: RDD[A], envelopeWindow: B): RDD[A] = {
      // Just intersection -- need to implement full coverage
      rdd.filter(element => element.intersects(envelopeWindow))
  }
}
