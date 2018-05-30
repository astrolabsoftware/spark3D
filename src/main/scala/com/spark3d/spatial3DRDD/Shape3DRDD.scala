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
package com.spark3d.spatial3DRDD

// Re-partitioning
import com.spark3d.spatialPartitioning.SpatialPartitioner
import com.spark3d.spatialPartitioning.OnionPartitioning
import com.spark3d.spatialPartitioning.OnionPartitioner

// 3D Objects
import com.spark3d.geometryObjects._
import com.spark3d.geometryObjects.Shape3D._

// Grids
import com.spark3d.utils.GridType
import com.spark3d.utils.GridType._

// Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.PairFlatMapFunction

/**
  * Class to handle generic 3D RDD.
  * The output type is T which extends the class Shape3D.
  */
abstract class Shape3DRDD[T<:Shape3D] extends Serializable {

  /** RDD containing the initial data formated as T. */
  val rawRDD : RDD[T]

  val isSpherical : Boolean

  /**
    * Apply any Spatial Partitioner to this.rawRDD[T], and return a RDD[T]
    * with the new partitioning.
    *
    * @param partitioner : (SpatialPartitioner)
    *   Spatial partitioner as defined in utils.GridType
    * @return (RDD[T]) RDD whose elements are T (Point3D, Sphere, etc...)
    *
    */
  def spatialPartitioning(partitioner: SpatialPartitioner) : RDD[T] = {
    partition(partitioner)
  }
  /**
    * Apply a spatial partitioning to this.rawRDD, and return a RDD[T]
    * with the new partitioning.
    * The list of available partitioning can be found in utils/GridType.
    * By default, the outgoing level of parallelism is the same as the incoming
    * one (i.e. same number of partitions).
    *
    * @param gridtype : (GridType)
    *   Type of partitioning to apply. See utils/GridType.
    * @param numPartitions : (Int)
    *   Number of partitions for the partitioned RDD. By default (-1), the
    *   number of partitions is that of the raw RDD. You can force it to be
    *   different by setting manually this parameter.
    *   Be aware of shuffling though...
    * @return (RDD[T]) RDD whose elements are T (Point3D, Sphere, etc...)
    *
    */
  def spatialPartitioning(gridtype : GridType, numPartitions : Int = -1) : RDD[T] = {

    val numPartitionsRaw = if (numPartitions == -1) {
      // Same number of partitions as the rawRDD
      rawRDD.getNumPartitions
    } else {
      // Force new partition number
      numPartitions
    }

    // Add here new cases.
    val partitioner = gridtype match {
      case GridType.LINEARONIONGRID => {
        // Initialise our space
        val partitioning = new OnionPartitioning
        partitioning.LinearOnionPartitioning(
          numPartitionsRaw,
          partitioning.getMaxZ(rawRDD),
          isSpherical
        )

        // Grab the grid elements
        val grids = partitioning.getGrids

        // Build our partitioner
        new OnionPartitioner(grids)
      }
      case _ => throw new AssertionError("""
        Unknown grid type! See utils.GridType for available grids.""")
    }

    // Apply the partitioner and return the RDD
    partition(partitioner)
  }

  /**
    * Repartion a RDD[T] according to a custom partitioner.
    * We follow the GeoSpark method for the moment (using the Java API).
    * In the future, that would be good to write that using the Scala API.
    *
    * @param rdd : (RDD[T])
    *   RDD of T (must extends Shape3D).
    * @param partitioner : (SpatialPartitioner)
    *   Instance of SpatialPartitioner or any extension of it.
    * @return (RDD[T]) Return a RDD[T] repartitioned.
    *
    */
  def partition(partitioner: SpatialPartitioner) : RDD[T] = {
    // RDD -> JavaRDD -> JavaPairRDD with custom partitioner
    rawRDD.toJavaRDD.flatMapToPair[Int, T](
      new PairFlatMapFunction[T, Int, T]() {
        override def call(spatialObject: T) : java.util.Iterator[Tuple2[Int, T]] = {
          partitioner.placeObject[T](spatialObject)
        }
      }
    // Partition the space, and override FlatMapFunction methods
    ).partitionBy(partitioner).mapPartitions(
      new FlatMapFunction[java.util.Iterator[Tuple2[Int, T]], T]() {
        override def call(tuple2Iterator : java.util.Iterator[Tuple2[Int, T]]) : java.util.Iterator[T] = {
          new java.util.Iterator[T]() {
            override def hasNext() : Boolean = tuple2Iterator.hasNext()

            override def next() : T = tuple2Iterator.next()._2

            override def remove() = throw new UnsupportedOperationException()
          }
        }
      }, true
    ).rdd
  }
}
