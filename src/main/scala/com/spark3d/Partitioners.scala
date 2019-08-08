/*
 * Copyright 2018 AstroLab Software
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

import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D
import com.astrolabsoftware.spark3d.geometryObjects.Point3D

import com.astrolabsoftware.spark3d.spatialPartitioning.SpatialPartitioner
import com.astrolabsoftware.spark3d.spatialPartitioning.OnionPartitioning
import com.astrolabsoftware.spark3d.spatialPartitioning.OnionPartitioner
import com.astrolabsoftware.spark3d.spatialPartitioning.Octree
import com.astrolabsoftware.spark3d.spatialPartitioning.OctreePartitioning
import com.astrolabsoftware.spark3d.spatialPartitioning.OctreePartitioner
import com.astrolabsoftware.spark3d.utils.Utils.getSampleSize
import com.astrolabsoftware.spark3d.spatialPartitioning.KDtree
import com.astrolabsoftware.spark3d.spatialPartitioning.KDtreePartitioning
import com.astrolabsoftware.spark3d.spatialPartitioning.KDtreePartitioner
/**
  * Main object to retrieve SpatialPartitioner.
  */
class Partitioners(df : DataFrame, options: Map[String, String]) extends Serializable {
   
  // Definition of the coordinate system. Spherical or cartesian
  val isSpherical : Boolean = options("coordSys") match {
    case "spherical" => true
    case "cartesian" => false
    case _ => throw new AssertionError("""
      Coordinate system not understood! You must choose between:
      spherical, cartesian
      """)
  }

  val colnames : Array[String] = options("colnames").split(",")

  val gridtype = options("gridtype")

  //
  val rawRDD = options("geometry") match {

    // Select the 3 columns (x, y, z) and cast to double
    case "points" => {
      df.select(
        col(colnames(0)).cast("double"),
        col(colnames(1)).cast("double"),
        col(colnames(2)).cast("double")
      ).rdd.map(x => new Point3D(
        x.getDouble(0), x.getDouble(1), x.getDouble(2), isSpherical)
      )
    }

    // Select the 4 columns (x, y, z, R) and cast to double
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
    case _ => throw new AssertionError("""
      Geometry not understood! You must choose between:
      points or spheres
      """)
  }       
     
      
         /**
    * Define a spatial partitioning for rawRDD, and return the partitioner.
    * The list of available partitioning can be found in utils/GridType.
    * By default, the outgoing level of parallelism is the same as the incoming
    * one (i.e. same number of partitions).
    *
    * This is typically here that you would add a new entry for new partitioners.
    *
    * @param numPartitions : (Int)
    *   Number of partitions for the partitioned RDD. By default (-1), the
    *   number of partitions is that of the raw RDD. You can force it to be
    *   different by setting manually this parameter.
    *   Be aware of shuffling though...!
    * @return the partitioner (SpatialPartitioner)
    *
    */
  def get(numPartitions : Int = -1) : SpatialPartitioner = {

    // Change the number of partitions if wanted
    val numPartitionsRaw = numPartitions match {
      case -1 => rawRDD.getNumPartitions
      case x if x > 0 => numPartitions
      case _ => throw new AssertionError(s"""
        The number of partitions must be strictly greater than zero!
        Otherwise leave it unset to take the number of partitions
        of the input DataFrame.
        (You put: $numPartitions)
        """)
    }
    
    // Add here new cases.
    val partitioner = gridtype match {
      case "onion" => {

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
       var x= new OnionPartitioner(grids)
        
       x
      }
      case "octree" => {
        // taking 20% of the data as a sample
        val dataCount = rawRDD.count
        val sampleSize = getSampleSize(dataCount, numPartitionsRaw)
        val samples = rawRDD.takeSample(false, sampleSize,
            new Random(dataCount).nextInt(dataCount.asInstanceOf[Int])).toList.map(x => x.getEnvelope)
        // see https://github.com/JulienPeloton/spark3D/issues/37
        // for the maxLevels and maxItemsPerNode calculations logic
        val maxLevels = floor(log(numPartitionsRaw)/log(8)).asInstanceOf[Int]
        val maxItemsPerBox = ceil(dataCount/pow(8, maxLevels + 1)).asInstanceOf[Int]
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
      //for KDtree
      case "kdtree" => {
        val dataCount = rawRDD.count
        val sampleSize = getSampleSize(dataCount, numPartitionsRaw)
        val samples:List[Point3D] = rawRDD.takeSample(false, sampleSize,
            new Random(dataCount).nextInt(dataCount.asInstanceOf[Int])).toList.map(x => x.asInstanceOf[Point3D]) 
         
        // To determine the level which is used in partitioning, also the number of partitions is determined
        // Number of partitions is power of 2. 1, 2, 4, 8, 16, 32 and so on
        val log2 = (x: Int) => floor(log10(x)/log10(2.0)).asInstanceOf[Int]
        val levelPartitioning=log2(numPartitionsRaw) +1  
         
        val kdtree=new KDtree( )  
        val partitioning = KDtreePartitioning.apply(samples, kdtree, levelPartitioning)
        val grids:List[BoxEnvelope] =  partitioning.getGrids 
        new KDtreePartitioner(kdtree,grids)
         
      }


      // Other cases not handled. RTree in prep.
      case _ => throw new AssertionError("""
        Unknown grid type! See utils.GridType for available grids.""")
    }

    // Apply the partitioner and return the RDD
     partitioner
   
  }

  /**
    * Try to estimate the enclosing box for the data set.
    * This is currently used to initialise the octree partitioning.
    *
    * @return dataBoundary (instance of BoxEnvelope)
    */
  def getDataEnvelope(): BoxEnvelope = {
    val seqOp: (BoxEnvelope, BoxEnvelope) => BoxEnvelope = {
      (x, y) => {
        // Check if x is initially null
        if (x.getEnvelope.isNull) {
          BoxEnvelope.apply(y.minX, y.maxX, y.minY, y.maxY, y.minZ, y.maxZ)
        } else {
          BoxEnvelope.apply(
            min(x.minX, y.minX), max(x.maxX, y.maxX),
            min(x.minY, y.minY), max(x.maxY, y.maxY),
            min(x.minZ, y.minZ), max(x.maxZ, y.maxZ)
          )
        }
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
 
