// /*
//  * Copyright 2018 Julien Peloton
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// package com.astrolabsoftware.spark3d.spatialOperator
//
// import scala.reflect.ClassTag
// import scala.collection.mutable.HashSet
//
// import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._
//
// import org.apache.spark.rdd.RDD
//
// /**
//   * Object containing routines to perform cross match between two sets A & B
//   * based on an underlying pixelisation. The space is first pixelised, and match
//   * is done between cells of this pixelisation.
//   * As a first example, we include the Healpix pixelisation (2D pixelisation).
//   * Each RDD partition is a shell, projected on a 2D plan, and pixelized.
//   *
//   */
// object PixelCrossMatch {
//
//   /**
//     * Perform a cross match between partition A and partition B based
//     * on the healpix index of object centers, and return all elements matching
//     * as tuple(A, B).
//     *
//     * @param iterA : (Iterator[A])
//     *   Iterator containing elements of A in a partition
//     * @param iterB : (Iterator[B])
//     *   Iterator containing elements of B in the same partition
//     * @param nside : (Int)
//     *   Resolution of the healpix grid.
//     * @return (Iterator[(A, B)]) iterator containing A and B elements matching.
//     *
//     */
//   def healpixMatchAndReturnAB[A<:Shape3D, B<:Shape3D](
//       iterA: Iterator[A], iterB: Iterator[B], nside: Int) : Iterator[(A, B)] = {
//
//     // Initialise containers
//     val result = HashSet.empty[(A, B)]
//
//     val elementsA = iterA.toList
//     val hpIndexAAndElements = elementsA.map(x => (x.toHealpix(nside), x ))
//
//     // Loop over elements of partition B, and match with elements of partition A
//     while (iterB.hasNext) {
//       val elementB = iterB.next()
//       val hpIndexB = elementB.toHealpix(nside)
//
//       val matched = hpIndexAAndElements
//         // Discriminate using HP indices
//         .filter(x => x._1 == hpIndexB)
//         // Keep only elements (drop indices)
//         .map(x => x._2)
//
//       if (matched.size > 0) {
//         for (el <- matched) {
//           result += Tuple2(el, elementB)
//         }
//       }
//     }
//     result.iterator
//   }
//
//   /**
//     * Perform a cross match between partition A and partition B based
//     * on the healpix index of object centers, and return elements of B which
//     * match with A.
//     *
//     * @param iterA : (Iterator[A])
//     *   Iterator containing elements of A in a partition
//     * @param iterB : (Iterator[B])
//     *   Iterator containing elements of B in the same partition
//     * @param nside : (Int)
//     *   Resolution of the healpix grid.
//     * @return (Iterator[B]) iterator containing elements of B matching with A.
//     *
//     */
//   def healpixMatchAndReturnB[A<:Shape3D, B<:Shape3D](
//       iterA: Iterator[A], iterB: Iterator[B], nside: Int) : Iterator[B] = {
//
//     // Initialise containers
//     val result = List.newBuilder[B]
//
//     // Keep only distinct Healpix indices
//     val elementsA = iterA.toList.map(x => x.toHealpix(nside)).distinct
//
//     // Loop over elements of partition B, and for each element search for a
//     // counterpart in A.
//     while (iterB.hasNext) {
//       val elementB = iterB.next()
//       val hpIndexB = elementB.toHealpix(nside)
//       val matched = elementsA.filter(x => x == hpIndexB).toList
//       if (matched.size > 0) {
//         result += elementB
//       }
//     }
//     result.result.iterator
//   }
//
//   /**
//     * Perform a cross match between partition A and partition B based
//     * on the healpix index of object centers, and return healpix indices found
//     * in both partitions. Duplicates are removed in the sense we do not keep
//     * the multiplicity for each pixel.
//     *
//     * @param iterA : (Iterator[A])
//     *   Iterator containing elements of A in a partition
//     * @param iterB : (Iterator[B])
//     *   Iterator containing elements of B in the same partition
//     * @param nside : (Int)
//     *   Resolution of the healpix grid.
//     * @return (Iterator[Long]) iterator containing healpix indices matching.
//     *
//     */
//   def healpixMatchAndReturnPixel[A<:Shape3D, B<:Shape3D](
//       iterA: Iterator[A], iterB: Iterator[B], nside: Int) : Iterator[Long] = {
//
//     // Initialise containers
//     val result = List.newBuilder[Long]
//
//     // Keep only distinct Healpix indices
//     val elementsA = iterA.toList.map(x => x.toHealpix(nside)).distinct
//     val elementsB = iterB.toList.map(x => x.toHealpix(nside)).distinct
//
//     // Sizes of each partition
//     val sizeB = elementsB.size
//
//     // Loop over elements of partition B, and match with elements of partition A
//     for (posB <- 0 to sizeB - 1) {
//       val hpIndexB = elementsB(posB)
//
//       val matched = elementsA.filter(x => x == hpIndexB).toList
//       if (matched.size > 0) {
//         result += hpIndexB
//       }
//     }
//     result.result.iterator
//   }
//
//   /**
//     * Cross match 2 RDD based on the healpix index of geometry center.
//     * The cross-match is done partition-by-partition, which means the two
//     * RDD must have been partitioned by the same partitioner.
//     * You have to choice to return:
//     *   (1) Elements of (A, B) matching (returnType="AB")
//     *   (2) Elements of A matching B (returnType="A")
//     *   (3) Elements of B matching A (returnType="B")
//     *   (4) Healpix pixel indices matching (returnType="healpix")
//     *
//     * Which one you should choose? That depends on what you need:
//     * (1) gives you all elements but is slow.
//     * (2) & (3) give you all elements only in one side but is faster.
//     * (4) gives you only healpix center but is even faster.
//     *
//     * @param rddA : (RDD[A<:Shape3D])
//     *   RDD whose elements are Shape3D or any extension (Point3D, ...)
//     * @param rddB : (RDD[B<:Shape3D])
//     *   RDD whose elements are Shape3D or any extension (Point3D, ...)
//     * @param nside : (Int)
//     *   Resolution of the underlying healpix map used to convert angle
//     *   coordinates to healpix index.
//     * @param returnType : (String)
//     *   Kind of crossmatch to perform:
//     *     - Elements of (A, B) matching (returnType="AB")
//     *     - Elements of A matching B (returnType="A")
//     *     - Elements of B matching A (returnType="B")
//     *     - Healpix pixel indices matching (returnType="healpix")
//     */
//   def CrossMatchHealpixIndex[A<:Shape3D : ClassTag, B<:Shape3D : ClassTag](
//       rddA: RDD[A], rddB: RDD[B], nside: Int, returnType: String = "B"): RDD[_] = {
//
//     // Check that the two RDD have the same partitioning.
//     if (rddA.partitioner != rddB.partitioner) {
//       throw new AssertionError("""
//         The two RDD must be partitioned by the same partitioner to perform
//         a cross-match! Use spatialPartitioning(rddA.partitioner) to apply
//         a spatial partitioning to a Shape3D RDD.
//         """
//       )
//     }
//
//     returnType match {
//       case "healpix" => rddA.zipPartitions(
//         rddB, true)((iterA, iterB) => healpixMatchAndReturnPixel(iterA, iterB, nside))
//       case "A" => rddB.zipPartitions(
//         rddA, true)((iterA, iterB) => healpixMatchAndReturnB(iterA, iterB, nside))
//       case "B" => rddA.zipPartitions(
//         rddB, true)((iterA, iterB) => healpixMatchAndReturnB(iterA, iterB, nside))
//       case "AB" => {
//         // This is more challenging as we return objects from both sides
//         // In order to balance speed-up vs memory, I use this combination:
//         // zipPartitions -> map -> aggregateByKey
//         rddA.zipPartitions(rddB, true)(
//           (iterA, iterB) => healpixMatchAndReturnAB(iterA, iterB, nside)
//         ).map(x => (x._1, x._2))
//         .aggregateByKey(new java.util.HashSet[B])(
//           (queue, item) => {
//             queue.add(item)
//             queue
//           },
//           (queue1, queue2) => {
//             queue1.addAll(queue2)
//             queue1
//           }
//         )
//       }
//       case _ => throw new AssertionError("""
//         I do not know how to perform the cross match.
//         Choose between: "A", "B", "AB", or "healpix".
//         """)
//     }
//   }
// }
