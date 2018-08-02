package com.spark3d.spatialPartitioning.Rtree

import com.spark3d.geometry.Envelope

class NonLeafNode (var envelope: Envelope,
                   var children: List[Node],
                   val level: Int) extends Node(envelope){

  def this(level: Int) {
    this(null, null, level)
  }
}
