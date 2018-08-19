package com.spark3d.spatialPartitioning.Rtree

import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope

import scala.collection.mutable.ListBuffer

abstract class Node extends Serializable {

  def envelope: BoxEnvelope

  def children: ListBuffer[Node]
}
