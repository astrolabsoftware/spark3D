package com.astrolabsoftware.spark3d.spatialPartitioning.Rtree

import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope

import scala.collection.mutable.ListBuffer

abstract class Node (var parent: Node = null) extends Serializable {

  def envelope: BoxEnvelope

  def children: ListBuffer[Node]
}
