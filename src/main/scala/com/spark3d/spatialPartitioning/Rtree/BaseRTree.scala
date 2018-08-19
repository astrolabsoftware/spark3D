package com.spark3d.spatialPartitioning.Rtree


import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D

import scala.math._
import scala.collection.mutable.ListBuffer

class BaseRTree (private val maxNodeCapacity: Int = 10){

  def this() {
    this(10)
  }


  var objectList: List[BoxEnvelope] = _
  var root: NonLeafNode = _
  var built: Boolean = false
//  private var maxNodeCapacity: Int = 10

  def insert(objList: List[BoxEnvelope]): Unit = {
    objectList = objList
  }

  def build(): Unit = {

    if (built) {
       return
    }

    root = if (objectList.isEmpty) {
      new NonLeafNode(0)
    } else {
      //build the tree
      buildUpperRTree(objectList.map(x => new LeafNode(x.getEnvelope)), -1)
    }

    built = true
  }

  private def buildUpperRTree(levelNodes: List[Node], level: Int): NonLeafNode = {

    val levelParents = constructParents(levelNodes, level+1)

    if (levelParents.size == 1) {
      return levelParents(0).asInstanceOf[NonLeafNode]
    }

    buildUpperRTree(levelParents, level+1)
  }

  private def constructParents(children: List[Node], level: Int): List[Node] = {

//    assert(children.foreach(x => x.isInstanceOf[Shape3D]))
    var parents = ListBuffer[Node]()

    parents += new NonLeafNode(level)

    val minLeafCount = max(ceil(children.size / maxNodeCapacity).toInt, 1)

    val sortedChildren = children.sortWith((x, y) =>
        x.envelope.center.x < y.envelope.center.x)

    val parentSlices = verticalSlices(sortedChildren, ceil(sqrt(minLeafCount)).toInt)
    createParentFromChildSlices(parentSlices, level)

  }

  def createParentFromChildSlices(parentSlices: List[List[Node]], level: Int): List[Node] = {
    val parents = ListBuffer[Node]()
    for (i <- parentSlices) {
      parents ++= createParentFromSlices(i, level)
    }
    parents.toList
  }

  def createParentFromSlices(parentSlices: List[Node], level: Int): List[Node] = {
    val parents = ListBuffer[NonLeafNode]()
    parents += new NonLeafNode(level)
    val it = parentSlices.iterator
    while (it.hasNext) {
      val parent = it.next
      if (parents.last.children.size == maxNodeCapacity) {
        parents += new NonLeafNode(level)
      }
      parents.last.children += parent
    }
    parents.toList
  }

  def verticalSlices(children: List[Node], sliceCount: Int): List[List[Node]] = {
    val sliceCapacity = (ceil(children.size) / sliceCount.asInstanceOf[Double]).asInstanceOf[Int]
    val it = children.iterator
    val slices = new Array[ListBuffer[Node]](sliceCount)
    for (i <- 0 until sliceCount) {
      slices(i) = ListBuffer[Node]()
      var added = 0
      while (it.hasNext && added < sliceCapacity) {
        val temp = it.next
        slices(i) += temp
        added += 1
      }
    }

    slices.toList.map(x => x.toList)
  }

  def getLeafNodes(): List[Node] = {
    val leafNodes = ListBuffer[Node]()
    getLeafNodes(root, leafNodes)
    leafNodes.toList
  }

  def getLeafNodes(node: Node, leafNodes: ListBuffer[Node]): Unit = {

    val leafNodes = ListBuffer[Node]()

    var isLeaf = false

    root.children.map(x => {
      if (x.isInstanceOf[LeafNode]){
        isLeaf = true
      }
    })

    if (isLeaf) {
      leafNodes += root
      return
    } else {
      for (child <- root.children) {
        getLeafNodes(child, leafNodes)
      }
    }
  }
}
