package com.spark3d.spatialPartitioning

import com.spark3d.geometry.BoxEnvelope

import scala.collection.mutable.ListBuffer

class OctreePartitioning (private val octree: Octree)
  extends Serializable {

  def getPartitionTree(): Octree = {
    octree
  }
}

object OctreePartitioning {

  def apply(data: ListBuffer[BoxEnvelope], boundry: BoxEnvelope): OctreePartitioning = {
    val tree = new Octree(boundry, 0)
    apply(data, tree)
  }

  def apply(data: ListBuffer[BoxEnvelope], tree: Octree): OctreePartitioning = {
    for (element <- data) {
      tree.insertElement(element)
    }
    tree.assignPartitionIDs
    new OctreePartitioning(tree)
  }
}
