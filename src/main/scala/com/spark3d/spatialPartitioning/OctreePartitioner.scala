package com.spark3d.spatialPartitioning

import com.spark3d.geometry.BoxEnvelope
import com.spark3d.geometryObjects.Shape3D.Shape3D

import scala.collection.mutable.{HashSet, ListBuffer}
import collection.JavaConverters._

class OctreePartitioner (octree: Octree, grids : List[BoxEnvelope]) extends SpatialPartitioner(grids) {

  override def numPartitions: Int = {
    grids.size
  }

  override def placeObject[T <: Shape3D](spatialObject: T): java.util.Iterator[Tuple2[Int, T]] = {

    val result = HashSet.empty[Tuple2[Int, T]]
    var matchedPartitions = new ListBuffer[BoxEnvelope]
    matchedPartitions ++= octree.getElements(spatialObject)
    for(partition <- matchedPartitions) {
      //to add partition.id
      result.add(new Tuple2(1, spatialObject))
    }
    result.toIterator.asJava
  }
}