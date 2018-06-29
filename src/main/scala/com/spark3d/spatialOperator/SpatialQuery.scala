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

import scala.collection.mutable.HashSet
import scala.collection.mutable.PriorityQueue
import scala.reflect.ClassTag


object SpatialQuery {

  /**
    * Finds the K nearest neighbors of the query object. The naive implementation here searches
    * through all the the objects in the RDD to get the KNN. The nearness of the objects here
    * is decided on the basis of the distance between their centers.
    *
    * @param queryObject object to which the knn are to be found
    * @param rdd RDD of a Shape3D (Shape3DRDD)
    * @param k number of nearest neighbors are to be found
    * @return knn
    */
  def KNN[A <: Shape3D: ClassTag, B <:Shape3D: ClassTag](queryObject: A, rdd: RDD[B], k: Int): List[B] = {

    // priority queue ordered by the distance to the query object.
    val pq: PriorityQueue[B] = PriorityQueue.empty[B](new GeometryObjectComparator[B](queryObject.center))
    val visited = new HashSet[Int]
    knnHelper[B](rdd, k,queryObject, pq, visited)
    // sort the list based on the closeness to the queryObject
    pq.toList.sortWith(_.center.distanceTo(queryObject.center) < _.center.distanceTo(queryObject.center))
  }

  /**
    * Much more efficient implementation of the KNN query above. First we seek the partitions in
    * which the query object belongs and we will look for the knn only in those partitions. After
    * this if the limit k is not satisfied, we keep looking similarly in the neighbors of the
    * containing partitions.
    *
    * @param queryObject object to which the knn are to be found
    * @param rdd RDD of a Shape3D (Shape3DRDD)
    * @param k number of nearest neighbors are to be found
    * @return knn
    */
  def KNNEfficient[A <: Shape3D: ClassTag, B <:Shape3D: ClassTag](queryObject: A, rdd: RDD[B], k: Int): List[B] = {

    // priority queue ordered by the distance to the query object.
    val pq: PriorityQueue[B] = PriorityQueue.empty[B](new GeometryObjectComparator[B](queryObject.center))
    // get the partitioner used for partitioning the input RDD
    val partitioner = rdd.partitioner.get.asInstanceOf[SpatialPartitioner]
    // get the partitions which contain the input object
    val containingPartitions = partitioner.getPartitionNodes(queryObject)
    // get the index of those partitions
    val containingPartitionsIndex = containingPartitions.map(x => x._1)
    // create a rdd of those partitions
    val matchedContainingSubRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (containingPartitionsIndex.contains(index)) iter else Iterator.empty
      }
    )

    val visited = new HashSet[Int]

    knnHelper[B](matchedContainingSubRDD, k, queryObject, pq, visited)

    // return if we found all k elements in the containing partitions
    if (pq.size >= k) {
      return pq.toList
    }

    // get the neighbor partitions to the partitions containing the input object.
    val neighborPartitions = partitioner.getNeighborNodes(queryObject)
    val neighborPartitionsIndex = neighborPartitions.map(x => x._1)

    // create a rdd of those partitions
    val matchedNeighborSubRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (neighborPartitionsIndex.contains(index)) iter else Iterator.empty
      }
    )

    knnHelper[B](matchedNeighborSubRDD, k, queryObject, pq, visited)
    // sort the list based on the closeness to the queryObject
    pq.toList.sortWith(_.center.distanceTo(queryObject.center) < _.center.distanceTo(queryObject.center))
  }

  /**
    * Helper function to iterate through all the objects of the input RDD and find k nearest
    * objects to the input object.
    *
    * @param rdd RDD of a Shape3D (Shape3DRDD)
    * @param k number of nearest neighbors are to be found
    * @param queryObject object to which the knn are to be found
    * @param pq Priority Queue for containing KNN
    */
  private def knnHelper[A <: Shape3D: ClassTag](rdd: RDD[A], k: Int,
    queryObject: Shape3D, pq: PriorityQueue[A], visited: HashSet[Int]): Unit = {

    val itr = rdd.toLocalIterator

    while (itr.hasNext) {
      val currentElement = itr.next
      if (!visited.contains(currentElement.center.getHash)) {
        if (pq.size < k) {
          pq.enqueue(currentElement)
          visited += currentElement.center.getHash
        } else {
          val currentEleDist = currentElement.center.distanceTo(queryObject.center)
          // TODO make use of pq.max
          val maxElement = pq.dequeue
          val maxEleDist = maxElement.center.distanceTo(queryObject.center)
          if (currentEleDist < maxEleDist) {
            pq.enqueue(currentElement)
            visited += currentElement.center.getHash
            visited -= maxElement.center.getHash
          } else {
            pq.enqueue(maxElement)
          }
        }
      }
    }
  }
}
