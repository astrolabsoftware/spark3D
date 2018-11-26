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

import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D
import com.astrolabsoftware.spark3d.utils.GeometryObjectComparator
import com.astrolabsoftware.spark3d.utils.Utils.takeOrdered
import com.astrolabsoftware.spark3d.spatialPartitioning._

import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashSet, ListBuffer}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.Breaks._

object KNN {

  /**
    * Finds the K nearest neighbors of the query object.
    * The naive implementation here searches through all the the objects in
    * the RDD to get the KNN. The nearness of the objects here
    * is decided on the basis of the distance between their centers.
    *
    * @param rdd : RDD[T]
    *   RDD of T (T<:Shape3D)
    * @param queryObject : (T<:Shape3D)
    *   Object to which the KNN are to be found
    * @param k : (Int)
    *   Number of nearest neighbors requested
    * @param unique : (Boolean)
          If True, returns only distinct objects. Default is false.
    * @return (List[T]) List of the KNN T<:Shape3D.
    *
    */
  def KNNStandard[T <: Shape3D: ClassTag](
      rdd: RDD[T], queryObject: T,
      k: Int, unique: Boolean = false): List[T] = {
    val knn = takeOrdered[T](rdd, k, queryObject, unique)(
      new GeometryObjectComparator[T](queryObject.center)
    )
    knn.toList
  }
}
