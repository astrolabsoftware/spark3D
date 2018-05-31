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

object PixelCrossMatch {

  /**
    * Perform a cross match between partition A and partition B based
    * on the healpix index of object centers, and return all elements matching
    * as tuple(A, B).
    *
    * @param iterA : (Iterator[A])
    *   Iterator containing elements of A in a partition
    * @param iterB : (Iterator[B])
    *   Iterator containing elements of B in the same partition
    * @param nside : (Int)
    *   Resolution of the healpix grid.
    * @return (Iterator[(A, B)]) iterator containing A and B elements matching.
    *
    */
  def healpixMatchAndReturnAB[A<:Shape3D, B<:Shape3D](
      iterA: Iterator[A], iterB: Iterator[B], nside: Int) : Iterator[(A, B)] = {

    // Initialise containers
    val result = List.newBuilder[(A, B)]
    val queryObjects = List.newBuilder[A]

    // Construct entire partition A
    while (iterA.hasNext) {
        queryObjects += iterA.next()
    }
    val elementsA = queryObjects.result
    val sizeA = elementsA.size

    // Loop over elements of partition B, and match with elements of partition A
    while (iterB.hasNext) {
      val elementB = iterB.next()
      val hpIndexB = elementB.toHealpix(nside)

      var pos = 0
      while (pos < sizeA) {
        val elementA = elementsA(pos)
        val hpIndexA = elementA.toHealpix(nside)
        if (hpIndexB == hpIndexA) {
          result += ((elementA, elementB))
        }
        // Update the position in the partition A
        pos += 1
      }
    }
    result.result.iterator
  }

  /**
    * Perform a cross match between partition A and partition B based
    * on the healpix index of object centers, and return elements of B which
    * match with A.
    *
    * @param iterA : (Iterator[A])
    *   Iterator containing elements of A in a partition
    * @param iterB : (Iterator[B])
    *   Iterator containing elements of B in the same partition
    * @param nside : (Int)
    *   Resolution of the healpix grid.
    * @return (Iterator[B]) iterator containing elements of B matching with A.
    *
    */
  def healpixMatchAndReturnB[A<:Shape3D, B<:Shape3D](
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
    * Perform a cross match between partition A and partition B based
    * on the healpix index of object centers, and return healpix indices found
    * in both partitions. Duplicates are removed in the sense we do not keep
    * the multiplicity for each pixel.
    *
    * @param iterA : (Iterator[A])
    *   Iterator containing elements of A in a partition
    * @param iterB : (Iterator[B])
    *   Iterator containing elements of B in the same partition
    * @param nside : (Int)
    *   Resolution of the healpix grid.
    * @return (Iterator[Long]) iterator containing healpix indices matching.
    *
    */
  def healpixMatchAndReturnPixel[A<:Shape3D, B<:Shape3D](
      iterA: Iterator[A], iterB: Iterator[B], nside: Int) : Iterator[Long] = {

    // Initialise containers
    val result = List.newBuilder[Long]
    val indicesA = List.newBuilder[Long]
    val indicesB = List.newBuilder[Long]

    // Construct entire partition A based on healpix index
    while (iterA.hasNext) {
        indicesA += iterA.next().toHealpix(nside)
    }

    // Construct entire partition B based on healpix index
    while (iterB.hasNext) {
        indicesB += iterB.next().toHealpix(nside)
    }

    // Remove duplicates within partitions
    val elementsA = indicesA.result.distinct
    val elementsB = indicesB.result.distinct

    // Sizes of each partition
    val sizeA = elementsA.size
    val sizeB = elementsB.size

    // Loop over elements of partition B, and match with elements of partition A
    for (posB <- 0 to sizeB - 1) {
      val hpIndexB = elementsB(posB)

      var posA = 0
      var found = false
      while (posA < sizeA && !found) {
        val hpIndexA = elementsA(posA)
        if (hpIndexB == hpIndexA) {
          result += hpIndexB

          // No duplicates, once a pixel has a match
          // we exit the search.
          found = true
        }

        // Update the position in the partition A
        posA += 1
      }
    }
    result.result.iterator
  }

  /**
    * Cross match 2 RDD based on the healpix index of geometry center.
    * You have to choice to return:
    *   (1) Elements of (A, B) matching (returnType="AB")
    *   (2) Elements of A matching B (returnType="A")
    *   (3) Elements of B matching A (returnType="B")
    *   (4) Healpix pixel indices matching (returnType="healpix")
    *
    * Which one you should choose? That depends on what you need:
    * (1) gives you all elements but is slow.
    * (2) & (3) give you all elements only in one side but is faster.
    * (4) gives you only healpix center but is even faster.
    *
    * @param rddA : (RDD[A<:Shape3D])
    *   RDD whose elements are Shape3D or any extension (Point3D, ...)
    * @param rddB : (RDD[B<:Shape3D])
    *   RDD whose elements are Shape3D or any extension (Point3D, ...)
    * @param nside : (Int)
    *   Resolution of the underlying healpix map used to convert angle
    *   coordinates to healpix index.
    * @param returnType : (String)
    *   Kind of crossmatch to perform:
    *     - Elements of (A, B) matching (returnType="AB")
    *     - Elements of A matching B (returnType="A")
    *     - Elements of B matching A (returnType="B")
    *     - Healpix pixel indices matching (returnType="healpix")
    */
  def CrossMatchHealpixIndex[A<:Shape3D : ClassTag, B<:Shape3D : ClassTag](
      rddA: RDD[A], rddB: RDD[B], nside: Int, returnType: String = "healpix"): RDD[_] = {
    returnType match {
      case "healpix" => rddA.zipPartitions(
        rddB, true)((iterA, iterB) => healpixMatchAndReturnPixel(iterA, iterB, nside))
      case "A" => rddB.zipPartitions(
        rddA, true)((iterA, iterB) => healpixMatchAndReturnB(iterA, iterB, nside))
      case "B" => rddA.zipPartitions(
        rddB, true)((iterA, iterB) => healpixMatchAndReturnB(iterA, iterB, nside))
      case "AB" => rddA.zipPartitions(
        rddB, true)((iterA, iterB) => healpixMatchAndReturnAB(iterA, iterB, nside))
      case _ => throw new AssertionError("""
        I do not know how to perform the cross match.
        Choose between: "A", "B", "AB", or "healpix".
        """)
    }
  }
}
