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
package com.astrolabsoftware.spark3d

import scala.math._
import scala.util.Random

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._

import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.spatialPartitioning.SpatialPartitioner
import com.astrolabsoftware.spark3d.spatialPartitioning.OnionPartitioning
import com.astrolabsoftware.spark3d.spatialPartitioning.OnionPartitioner
import com.astrolabsoftware.spark3d.spatialPartitioning.Octree
import com.astrolabsoftware.spark3d.spatialPartitioning.OctreePartitioning
import com.astrolabsoftware.spark3d.spatialPartitioning.OctreePartitioner
import com.astrolabsoftware.spark3d.utils.Utils.getSampleSize

/**
  * Main object to retrieve SpatialPartitioner.
  */
class Partitioners(df : DataFrame, options: Map[String, String]) extends Serializable {

  val isSpherical : Boolean = options("coordSys") match {
    case "spherical" => true
    case _ => false
  }

  val colnames : Array[String] = options("colnames").split(",")

  val gridtype = options("gridtype")

  // Select the 3 columns (x, y, z)
  // and cast to double in case.
  val rawRDD = options("geometry") match {
    case "points" => {
      df.select(
        col(colnames(0)).cast("double"),
        col(colnames(1)).cast("double"),
        col(colnames(2)).cast("double")
      ).rdd.map(x => new Point3D(
        x.getDouble(0), x.getDouble(1), x.getDouble(2), isSpherical)
      )
    }
    case "spheres" => {
      df.select(
        col(colnames(0)).cast("double"),
        col(colnames(1)).cast("double"),
        col(colnames(2)).cast("double"),
        col(colnames(3)).cast("double")
      ).rdd.map(x => new ShellEnvelope(
        x.getDouble(0), x.getDouble(1), x.getDouble(2), isSpherical, x.getDouble(3))
      )
    }
  }


  /**
    * Apply a spatial partitioning to this.rawRDD, and return a RDD[T]
    * with the new partitioning.
    * The list of available partitioning can be found in utils/GridType.
    * By default, the outgoing level of parallelism is the same as the incoming
    * one (i.e. same number of partitions).
    *
    * @param gridtype : (String)
    *   Type of partitioning to apply. See utils/GridType.
    * @param numPartitions : (Int)
    *   Number of partitions for the partitioned RDD. By default (-1), the
    *   number of partitions is that of the raw RDD. You can force it to be
    *   different by setting manually this parameter.
    *   Be aware of shuffling though...
    * @return (RDD[T]) RDD whose elements are T (Point3D, Sphere, etc...)
    *
    */
  def get(numPartitions : Int = -1) : SpatialPartitioner = {

    val numPartitionsRaw = if (numPartitions == -1) {
      // Same number of partitions as the rawRDD
      rawRDD.getNumPartitions
    } else {
      // Force new partition number
      numPartitions
    }

    // Add here new cases.
    val partitioner = gridtype match {
      case "LINEARONIONGRID" => {
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
      case "OCTREE" => {
        // taking 20% of the data as a sample
        val dataCount = rawRDD.count
        val sampleSize = getSampleSize(dataCount, numPartitions)
        val samples = rawRDD.takeSample(false, sampleSize,
            new Random(dataCount).nextInt(dataCount.asInstanceOf[Int])).toList.map(x => x.getEnvelope)
        // see https://github.com/JulienPeloton/spark3D/issues/37
        // for the maxLevels and maxItemsPerNode calculations logic
        val maxLevels = floor(log(numPartitionsRaw)/log(8)).asInstanceOf[Int]
        val maxItemsPerBox = ceil(dataCount/pow(8, maxLevels)).asInstanceOf[Int]
        if (maxItemsPerBox > Int.MaxValue) {
          throw new AssertionError(
            """
              The max number of elements per partition have become greater than Int limit.
              Consider increasing the number of partitions.
            """)
        }
        val octree = new Octree(getDataEnvelope, 0, null, maxItemsPerBox, maxLevels)
        val partitioning = OctreePartitioning.apply(samples, octree)
        val grids = partitioning.getGrids
        new OctreePartitioner(octree, grids)
    }
      case _ => throw new AssertionError("""
        Unknown grid type! See utils.GridType for available grids.""")
    }

    // Apply the partitioner and return the RDD
    partitioner
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

    val combOp: (BoxEnvelope, Shape3D) => BoxEnvelope = {
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

    val dataBoundary = rawRDD.aggregate(bx)(combOp, seqOp)

    // expand the boundary to also Include the elements at the border
    dataBoundary.expandOutwards(0.001)

    dataBoundary
  }

}
