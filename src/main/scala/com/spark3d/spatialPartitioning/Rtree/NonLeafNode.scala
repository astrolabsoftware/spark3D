package com.astrolabsoftware.spark3d.spatialPartitioning.Rtree

import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope

import scala.collection.mutable.ListBuffer

class NonLeafNode (var children: ListBuffer[Node],
                   val level: Int) extends Node {

  def this(level: Int) {
    this(ListBuffer[Node](), level)
  }

  override def envelope: BoxEnvelope = {
    if (children == null || children.isEmpty ) {
      return null
    }

    val boxEnv = new BoxEnvelope()
    boxEnv.minX = Int.MaxValue
    boxEnv.maxX = Int.MinValue
    boxEnv.minY = Int.MaxValue
    boxEnv.maxY = Int.MinValue
    boxEnv.minZ = Int.MaxValue
    boxEnv.maxZ = Int.MinValue
    val boundary = children.foldLeft(boxEnv)(
      (acc, child) => {
        if (acc.minX > child.envelope.minX) {
          acc.minX = child.envelope.minX
        }
        if (acc.maxX < child.envelope.maxX) {
          acc.maxX = child.envelope.maxX
        }
        if (acc.minY > child.envelope.minY) {
          acc.minY = child.envelope.minY
        }
        if (acc.maxY < child.envelope.maxY) {
          acc.maxY = child.envelope.maxY
        }
        if (acc.minZ > child.envelope.minZ) {
          acc.minZ = child.envelope.minZ
        }
        if (acc.maxZ < child.envelope.maxZ) {
          acc.maxZ = child.envelope.maxZ
        }
        acc
      }
    )
    boundary
  }

}
