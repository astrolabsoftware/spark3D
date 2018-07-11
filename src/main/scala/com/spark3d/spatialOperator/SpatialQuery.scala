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

package com.astrolabsoftware.spark3d.spatialOperator

import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D
import com.astrolabsoftware.spark3d.utils.GeometryObjectComparator
import com.astrolabsoftware.spark3d.utils.Utils.takeOrdered
import org.apache.spark.rdd.RDD
import com.astrolabsoftware.spark3d.spatialPartitioning._

import scala.collection.mutable.{HashSet, ListBuffer, PriorityQueue}
import scala.reflect.ClassTag
import scala.util.control.Breaks._

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
  def KNN[T <: Shape3D: ClassTag](queryObject: T, rdd: RDD[T], k: Int, unique: Boolean = false): List[T] = {
//    val knn = rdd.takeOrdered(k)(new GeometryObjectComparator[B](queryObject.center))
    val knn = takeOrdered[T](rdd, k, queryObject, unique)(new GeometryObjectComparator[T](queryObject.center))
    knn.toList
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

    val partitioner = rdd.partitioner.get.asInstanceOf[SpatialPartitioner]
    val containingPartitions = partitioner.getPartitionNodes(queryObject)
    val containingPartitionsIndex = containingPartitions.map(x => x._1)
    val matchedContainingSubRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (containingPartitionsIndex.contains(index)) iter else Iterator.empty
      }
    )

    val knn_1 = matchedContainingSubRDD.takeOrdered(k)(new GeometryObjectComparator[B](queryObject.center))

    if (knn_1.size >= k) {
      return knn_1.toList
    }

    val visitedPartitions = new HashSet[Int]
    visitedPartitions ++= containingPartitionsIndex

    val neighborPartitions = partitioner.getNeighborNodes(queryObject)
        .filter(x => !visitedPartitions.contains(x._1)).to[ListBuffer]
    val neighborPartitionsIndex = neighborPartitions.map(x => x._1)

    val matchedNeighborSubRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (neighborPartitionsIndex.contains(index)) iter else Iterator.empty
      }
    )

    val knn_2 = matchedNeighborSubRDD.takeOrdered(k-knn_1.size)(new GeometryObjectComparator[B](queryObject.center))

    var knn_f = knn_1 ++ knn_2
    if (knn_f.size >= k) {
      return knn_f.toList
    }

    visitedPartitions ++= neighborPartitionsIndex

    breakable {
      for (neighborPartition <- neighborPartitions) {
        val secondaryNeighborPartitions = partitioner.getSecondaryNeighborNodes(neighborPartition._2, neighborPartition._1)
            .filter(x => !visitedPartitions.contains(x._1))
        val secondaryNeighborPartitionsIndex = secondaryNeighborPartitions.map(x => x._1)

        val matchedSecondaryNeighborSubRDD = rdd.mapPartitionsWithIndex(
          (index, iter) => {
            if (secondaryNeighborPartitionsIndex.contains(index))
              iter
            else
              Iterator.empty
          }
        )


        val knn_t = matchedNeighborSubRDD.takeOrdered(k-knn_f.size)(new GeometryObjectComparator[B](queryObject.center))

        knn_f = knn_f ++ knn_t

        if (knn_f.size >= k) {
          break
        }

        visitedPartitions ++= secondaryNeighborPartitionsIndex
        neighborPartitions ++= secondaryNeighborPartitions
      }
    }
    knn_f.toList
  }

}
