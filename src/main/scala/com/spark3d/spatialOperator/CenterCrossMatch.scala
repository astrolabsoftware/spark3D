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

/**
  * Object containing routines to perform cross match between two sets A & B
  * based on the position of object centers.
  */
object CenterCrossMatch {

  /**
    * Perform a cross match between partition A and partition B based
    * on the object centers, and return elements of B which
    * match with A.
    *
    * @param iterA : (Iterator[A])
    *   Iterator containing elements of A in a partition
    * @param iterB : (Iterator[B])
    *   Iterator containing elements of B in the same partition
    * @param epsilon : (Double)
    *   Tolerance for the distance between 2 centers. Should have the same
    *   units as the center coordinates.
    * @return (Iterator[B]) iterator containing elements of B matching with A.
    *
    */
  def centerMatchAndReturnB[A<:Shape3D, B<:Shape3D](
      iterA: Iterator[A], iterB: Iterator[B], epsilon: Double) : Iterator[B] = {

    // Initialise containers
    val result = List.newBuilder[B]

    val elementsA = iterA.toList
    val sizeA = elementsA.size

    // Loop over elements of partition B, and match with elements of partition A
    while (iterB.hasNext) {
      val elementB = iterB.next()

      val matched = elementsA.filter(x => elementB.hasCenterCloseTo(x.center, epsilon)).toList
      if (matched.size > 0) {
        result += elementB
      }
    }
    result.result.iterator
  }

  /**
    * Perform a full cross match between partition A and partition B based
    * on the object centers.
    *
    * @param iterA : (Iterator[A])
    *   Iterator containing elements of A in a partition
    * @param iterB : (Iterator[B])
    *   Iterator containing elements of B in the same partition
    * @param epsilon : (Double)
    *   Tolerance for the distance between 2 centers. Should have the same
    *   units as the center coordinates.
    * @return (Iterator[(A, B)]) iterator containing elements matching in both.
    *
    */
  def centerMatchAndReturnAB[A<:Shape3D, B<:Shape3D](
      iterA: Iterator[A], iterB: Iterator[B], epsilon: Double) : Iterator[(A, B)] = {

    // Initialise containers
    val result = List.newBuilder[(A, B)]

    val elementsA = iterA.toList
    val sizeA = elementsA.size

    // Loop over elements of partition B, and match with elements of partition A
    while (iterB.hasNext) {
      val elementB = iterB.next()

      val matched = elementsA.filter(x => elementB.hasCenterCloseTo(x.center, epsilon)).toList
      if (matched.size > 0) {
        for (el <- matched) {
          result += ((el, elementB))
        }
      }

    }
    result.result.iterator
  }

  /**
    * Cross match 2 RDD based on the object centers.
    * You have to choice to return:
    *   (1) Elements of (A, B) matching (returnType="AB")
    *   (2) Elements of A matching B (returnType="A")
    *   (3) Elements of B matching A (returnType="B")
    *
    * Which one you should choose? That depends on what you need:
    * (1) gives you all elements but is slow.
    * (2) & (3) give you all elements only in one side but is faster.
    *
    * @param rddA : (RDD[A<:Shape3D])
    *   RDD whose elements are Shape3D or any extension (Point3D, ...)
    * @param rddB : (RDD[B<:Shape3D])
    *   RDD whose elements are Shape3D or any extension (Point3D, ...)
    * @param epsilon : (Double)
    *   Tolerance for the distance between 2 centers. Should have the same
    *   units as the center coordinates.
    * @param returnType : (String)
    *   Kind of crossmatch to perform:
    *     - Elements of (A, B) matching (returnType="AB")
    *     - Elements of A matching B (returnType="A")
    *     - Elements of B matching A (returnType="B")
    */
  def CrossMatchCenter[A<:Shape3D : ClassTag, B<:Shape3D : ClassTag](
      rddA: RDD[A], rddB: RDD[B], epsilon: Double, returnType: String = "B"): RDD[_] = {

    // Check that the two RDD have the same partitioning.
    if (rddA.partitioner != rddB.partitioner) {
      throw new AssertionError("""
        The two RDD must be partitioned by the same partitioner to perform
        a cross-match! Use spatialPartitioning(rddA.partitioner) to apply
        a spatial partitioning to a Shape3D RDD.
        """
      )
    }

    // Catch unphysical conditions
    if (epsilon < 0.0) {
      throw new AssertionError("""
        Distance between objects cannot be negative.
        epsilon parameter must be positive!
        """)
    }

    returnType match {
      case "A" => rddB.zipPartitions(
        rddA, true)((iterA, iterB) => centerMatchAndReturnB(iterA, iterB, epsilon))
      case "B" => rddA.zipPartitions(
        rddB, true)((iterA, iterB) => centerMatchAndReturnB(iterA, iterB, epsilon))
      case "AB" => rddA.zipPartitions(
        rddB, true)((iterA, iterB) => centerMatchAndReturnAB(iterA, iterB, epsilon))
      case _ => throw new AssertionError("""
        I do not know how to perform the cross match.
        Choose between: "A", "B", or "AB".
        """)
    }
  }
}
