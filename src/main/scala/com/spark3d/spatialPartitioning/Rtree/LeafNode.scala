package com.astrolabsoftware.spark3d.spatialPartitioning.Rtree

import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope

import scala.collection.mutable.ListBuffer

class LeafNode (var env: BoxEnvelope) extends Node {

  override def envelope: BoxEnvelope = env

  override def children: ListBuffer[Node] = null

}
