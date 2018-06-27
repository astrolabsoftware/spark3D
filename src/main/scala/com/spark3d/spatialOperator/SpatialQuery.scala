/*
 * Copyright 2018 Mayur Bhosale
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

import com.spark3d.geometryObjects.Shape3D.Shape3D
import com.spark3d.utils.GeometryObjectComparator
import org.apache.spark.rdd.RDD
import com.spark3d.spatialPartitioning._

import scala.collection.mutable.PriorityQueue
import scala.reflect.ClassTag


object SpatialQuery {

  def KNN[A <: Shape3D: ClassTag, B <:Shape3D: ClassTag](queryObject: A, rdd: RDD[B], k: Int): List[B] = {

    val pq: PriorityQueue[B] = PriorityQueue.empty[B](new GeometryObjectComparator[B](queryObject.center))

    knnHelper[B](rdd, k,queryObject, pq)
    pq.toList.sortWith(_.center.distanceTo(queryObject.center) < _.center.distanceTo(queryObject.center))
  }

  def KNNEfficient[A <: Shape3D: ClassTag, B <:Shape3D: ClassTag](queryObject: A, rdd: RDD[B], k: Int): List[B] = {
    val pq: PriorityQueue[B] = PriorityQueue.empty[B](new GeometryObjectComparator[B](queryObject.center))

    val partitioner = rdd.partitioner.get.asInstanceOf[SpatialPartitioner]

    val containingPartitions = partitioner.getPartitionNodes(queryObject)
    val containingPartitionsIndex = containingPartitions.map(x => x._1)

    val matchedContainingSubRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (containingPartitionsIndex.contains(index)) iter else Iterator.empty
      }
    )

    knnHelper[B](matchedContainingSubRDD, k, queryObject, pq)

    if (pq.size >= k) {
      return pq.toList
    }

    val neighborPartitions = partitioner.getNeighborNodes(queryObject)
    val neighborPartitionsIndex = neighborPartitions.map(x => x._1)

    val matchedNeighborSubRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (neighborPartitionsIndex.contains(index)) iter else Iterator.empty
      }
    )

    knnHelper[B](matchedNeighborSubRDD, k, queryObject, pq)
    pq.toList.sortWith(_.center.distanceTo(queryObject.center) < _.center.distanceTo(queryObject.center))
  }

  private def knnHelper[A <: Shape3D: ClassTag](rdd: RDD[A], k: Int,
      queryObject: Shape3D, pq: PriorityQueue[A]): Unit = {

    val itr = rdd.toLocalIterator

    while (itr.hasNext) {
      val currentElement = itr.next
      if (pq.size < k) {
        pq.enqueue(currentElement)
      } else {
        val currentEleDist = currentElement.center.distanceTo(queryObject.center)
        // TODO make use of pq.max
        val maxElement = pq.dequeue
        val maxEleDist = maxElement.center.distanceTo(queryObject.center)
        if (currentEleDist < maxEleDist) {
          pq.enqueue(currentElement)
        } else {
          pq.enqueue(maxElement)
        }
      }
    }
  }
}
