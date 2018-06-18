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

// For implicits
import scala.reflect.ClassTag
import scala.math._

// Re-partitioning
import com.spark3d.spatialPartitioning.SpatialPartitioner
import com.spark3d.spatialPartitioning.OnionPartitioning
import com.spark3d.spatialPartitioning.OnionPartitioner
import com.spark3d.spatialPartitioning.Octree
import com.spark3d.spatialPartitioning.OctreePartitioning
import com.spark3d.spatialPartitioning.OctreePartitioner

// 3D Objects
import com.spark3d.geometryObjects._
import com.spark3d.geometryObjects.Shape3D._

// Grids
import com.spark3d.utils.GridType
import com.spark3d.utils.GridType._

// Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions

/**
  * Class to handle generic 3D RDD.
  * The output type is T which extends the class Shape3D.
  */
abstract class Shape3DRDD[T<:Shape3D] extends Serializable {

  /** RDD containing the initial data formated as T. */
  val rawRDD : RDD[T]

  val isSpherical : Boolean

  var boundary: BoxEnvelope = null
  /**
    * Apply any Spatial Partitioner to this.rawRDD[T], and return a RDD[T]
    * with the new partitioning.
    *
    * @param partitioner : (SpatialPartitioner)
    *   Spatial partitioner as defined in utils.GridType
    * @return (RDD[T]) RDD whose elements are T (Point3D, Sphere, etc...)
    *
    */
  def spatialPartitioning(partitioner: SpatialPartitioner)(implicit c: ClassTag[T]) : RDD[T] = {
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
  def spatialPartitioning(gridtype : GridType, numPartitions : Int = -1)(implicit c: ClassTag[T]) : RDD[T] = {

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
      case GridType.OCTREE => {
        // taking 20% of the data as a sample
        val sampleSize = (rawRDD.count * 0.2).asInstanceOf[Int]
        val samples = rawRDD.takeSample(false, sampleSize, 12).toList.map(x => x.getEnvelope)
        // see https://github.com/JulienPeloton/spark3D/issues/37
        // for the maxLevels and maxItemsPerNode calculations logic
        val maxLevels = floor(log(numPartitionsRaw)/log(8)).asInstanceOf[Int]
        val maxItemsPerBox = ceil(sampleSize/pow(8, maxLevels)).asInstanceOf[Int]
        val octree = new Octree(getDataEnvelope, 0, maxItemsPerBox, maxLevels)
        val partitioning = OctreePartitioning.apply(samples, octree)
        val grids = partitioning.getGrids
        new OctreePartitioner(octree, grids)
    }
      case _ => throw new AssertionError("""
        Unknown grid type! See utils.GridType for available grids.""")
    }

    // Apply the partitioner and return the RDD
    partition(partitioner)
  }

  /**
    * Repartion a RDD[T] according to a custom partitioner.
    *
    * @param rdd : (RDD[T])
    *   RDD of T (must extends Shape3D) with any partitioning.
    * @param partitioner : (SpatialPartitioner)
    *   Instance of SpatialPartitioner or any extension of it.
    * @return (RDD[T]) Repartitioned RDD[T].
    *
    */
  def partition(partitioner: SpatialPartitioner)(implicit c: ClassTag[T]) : RDD[T] = {
    // Go from RDD[V] to RDD[(K, V)] where K is specified by the partitioner.
    // Finally, return only RDD[V] with the new partitioning.
    new PairRDDFunctions[Int, T](
      rawRDD.map(x => partitioner.placeObject(x).next())
    ).partitionBy(partitioner).mapPartitions(_.map(_._2), true)
  }

  def getDataEnvelope(): BoxEnvelope = {
    val seqOp: (BoxEnvelope, BoxEnvelope) => BoxEnvelope = {
      (x, y) => {
        BoxEnvelope.apply(
          min(x.minX, y.minX), max(x.maxX, y.maxX),
          min(x.minY, y.minY), max(x.maxY, y.maxY),
          min(x.minZ, y.minZ), max(x.maxZ, y.maxZ)
        )
      }
    }

    val combOp: (BoxEnvelope, T) => BoxEnvelope = {
      (obj1, obj2) => {
        var x = obj1
        val y = obj2.getEnvelope
        if (x.isNull) {
          x = y
        }
        seqOp(x, y)
      }
    }

    // create a dummy envelope
    val bx = BoxEnvelope.apply(0, 0, 0, 0, 0, 0)
    // set it to null
    bx.setToNull

    rawRDD.aggregate(bx)(combOp, seqOp)
  }
}
