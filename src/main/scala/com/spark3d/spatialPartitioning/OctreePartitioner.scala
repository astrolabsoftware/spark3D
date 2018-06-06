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
package com.spark3d.spatialPartitioning

import com.spark3d.geometry.BoxEnvelope
import com.spark3d.geometryObjects.Shape3D.Shape3D

import scala.collection.mutable.{HashSet, ListBuffer}
import collection.JavaConverters._

class OctreePartitioner (octree: Octree, grids : List[BoxEnvelope]) extends SpatialPartitioner(grids) {

  override def numPartitions: Int = {
    grids.size
  }

  /**
    * Gets the object where the
    * @param spatialObject : (T<:Shape3D)
    *   Object of type T = Shape3D, or any extension like Point3D, Sphere, ...
    * @tparam T
    * @return (java.util.Iterator[Tuple2[Int, T]]) Java Iterator over
    *   a tuple (Key, Object). Key represents the partition number to which the
    *   spatialObject T belongs to.
    *
    */
  override def placeObject[T <: Shape3D](spatialObject: T): java.util.Iterator[Tuple2[Int, T]] = {

    val result = HashSet.empty[Tuple2[Int, T]]
    var matchedPartitions = new ListBuffer[BoxEnvelope]
    matchedPartitions ++= octree.getMatchedLeaves(spatialObject.getEnvelope)
    for(partition <- matchedPartitions) {
      result.add(new Tuple2(partition.indexID, spatialObject))
    }
    result.toIterator.asJava
  }
}