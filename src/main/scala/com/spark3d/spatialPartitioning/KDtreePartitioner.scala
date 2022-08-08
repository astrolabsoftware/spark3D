/*
 * Copyright 2019 AstroLab Software
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
package com.astrolabsoftware.spark3d.spatialPartitioning

 

import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope
import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D

import scala.collection.mutable.{HashSet, ListBuffer}

/**
 * @param kdtree KDtree tree
 * @param grids the list of boundary boxex/partitions in this partitioning
 * 
 */

class KDtreePartitioner (kdtree: KDtree, grids : List[BoxEnvelope]) extends SpatialPartitioner(grids) {
   
    
    
  /**
    * Get the number of partitions in this partitioning
    *
    * @return the number of partitions
    */
  override def numPartitions: Int = {
     grids.size
    
  }

  /**
    * 
    *
    */
  override def placeObject[T <: Shape3D](spatialObject: T): Iterator[Tuple2[Int, T]] = {
  
    null
  }

  /**
    *Assigning the tuple to its partitions
    *@param c0 the x-coordinate of the point
    *@param c1 the y-coordinate of the point
    *@param c2 the z-coordinate of the point
    @param isSpherical Definition of the coordinate system. Spherical or cartesian
    @return The partiton number of the tuple/3Dpoint
    * @param spatialObject : (T<:Shape3D)
    */

  override def placePoints(c0: Double, c1: Double, c2: Double, isSpherical: Boolean) : Int = {
     val result = HashSet.empty[Int]
     var partitionId:Int=0
     for(element<-grids){
       if(element.covers(c0,c1,c2)){
         return  partitionId
       }
       partitionId+=1
     }

     partitionId=0
     for(element<-grids){
       if(element.coversKD(c0,c1,c2)){
         return  partitionId
      }
       partitionId+=1
    }
        
      0  

  }

  /**
    * 
    */
  override def getPartitionNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = {
    null
  }

  /**
    * 
    */
  override def getNeighborNodes[T <: Shape3D](spatialObject: T, inclusive: Boolean = false): List[Tuple2[Int, Shape3D]] = {
    null
  }

  /**
   * 
   */
  override def getSecondaryNeighborNodes[T <: Shape3D](containingNode: T, containingNodeID: Int): List[Tuple2[Int, Shape3D]] = {
     
   null
  }

}
