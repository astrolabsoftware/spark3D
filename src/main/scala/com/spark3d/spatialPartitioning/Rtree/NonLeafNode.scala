package com.spark3d.spatialPartitioning.Rtree

import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope

import scala.collection.mutable.ListBuffer

class NonLeafNode (var children: ListBuffer[Node],
                   val level: Int) extends Node {

  def this(level: Int) {
    this(ListBuffer[Node](), level)
  }

  override def envelope: BoxEnvelope = {
//    ToDo: Implement the envelope property for the internal node (viz, non-leaf nodes)
    children.last.envelope
  }

}
