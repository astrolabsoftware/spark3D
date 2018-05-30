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

object CrossMatch {

  def healpixMatch[A<:Shape3D, B<:Shape3D](
      iterA: Iterator[A], iterB: Iterator[B], nside: Int) : Iterator[B] = {

    // Initialise containers
    val result = List.newBuilder[B]
    val queryObjects = List.newBuilder[Long]

    // Construct entire partition A
    while (iterA.hasNext) {
        queryObjects += iterA.next().toHealpix(nside)
    }

    // Remove duplicates in partition A
    val elementsA = queryObjects.result.distinct
    val sizeA = elementsA.size

    // Loop over elements of partition B, and match with elements of partition A
    while (iterB.hasNext) {
      val elementB = iterB.next()
      val hpIndexB = elementB.toHealpix(nside)

      var pos = 0
      while (pos < sizeA) {
        val hpIndexA = elementsA(pos)
        if (hpIndexB == hpIndexA) {
          result += elementB
        }
        // Update the position in the partition A
        pos += 1
      }
    }
    result.result.iterator
  }

  /**
    * Cross match 2 RDD based on the healpix index of geometry center.
    * Return only distinct healpix indices in common (no duplicates).
    *
    * @param rddA : (RDD[A<:Shape3D])
    *   RDD whose elements are Shape3D or any extension (Point3D, ...)
    * @param rddB : (RDD[B<:Shape3D])
    *   RDD whose elements are Shape3D or any extension (Point3D, ...)
    * @param nside : (Int)
    *   Resolution of the underlying healpix map used to convert angle
    *   coordinates to healpix index.
    */
  def CrossMatchHealpixIndex[A<:Shape3D : ClassTag, B<:Shape3D : ClassTag](
      rddA: RDD[A], rddB: RDD[B], nside: Int): RDD[B] = {
    rddA.zipPartitions(rddB, true)((iterA, iterB) => healpixMatch(iterA, iterB, nside))
  }
}
