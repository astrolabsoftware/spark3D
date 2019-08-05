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
package com.astrolabsoftware.spark3d.spatialPartitioning

 

import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope
import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D

import scala.collection.mutable.{HashSet, ListBuffer}

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
    * Gets the iterator on tuple leaf nodes (partitions) which intersects, contains or are contained
    * by the input object.
    *
    * @param spatialObject : (T<:Shape3D)
    *   Shape3D instance (or any extension) representing objects to put on
    *   the grid.
    * @return (Iterator[Tuple2[Int, T]) Iterable over a Tuple
    *         *   of (Int, T) where Int is the partition index, and T the input object.
    *
    */
  override def placeObject[T <: Shape3D](spatialObject: T): Iterator[Tuple2[Int, T]] = {
  
    null
  }

  /**
    * Gets the iterator on tuple leaf nodes (partitions) which intersects, contains or are contained
    * by the input object.
    *
    * @param spatialObject : (T<:Shape3D)
    *   Shape3D instance (or any extension) representing objects to put on
    *   the grid.
    * @return (Iterator[Tuple2[Int, T]) Iterable over a Tuple
    *         *   of (Int, T) where Int is the partition index, and T the input object.
    *
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
    * Gets the partitions which contain the input object.
    *
    * @param spatialObject input object for which the containment is to be found
    * @return list of Tuple of containing partitions and their index/partition ID's
    */
  override def getPartitionNodes[T <: Shape3D](spatialObject: T): List[Tuple2[Int, Shape3D]] = {
    null
  }

  /**
    * Gets the partitions which are the neighbors of the partitions which contain the input object.
    *
    * @param spatialObject input object for which the neighbors are to be found
    * @param inclusive If true, includes the node of the spatialObject as well. Default is false.
    * @return list of Tuple of neighbor partitions and their index/partition ID's
    */
  override def getNeighborNodes[T <: Shape3D](spatialObject: T, inclusive: Boolean = false): List[Tuple2[Int, Shape3D]] = {
    null
  }

  /**
    * Gets the partitions which are the neighbors to the input partition. Useful when getting
    * secondary neighbors (neighbors to neighbor) of the queryObject.
    *
    * @param containingNode The boundary of the Node for which neighbors are to be found.
    * @param containingNodeID The index/partition ID of the containingNode
    * @return list of Tuple of secondary neighbor partitions and their index/partition IDs
    */
  override def getSecondaryNeighborNodes[T <: Shape3D](containingNode: T, containingNodeID: Int): List[Tuple2[Int, Shape3D]] = {
     
   null
  }

}
