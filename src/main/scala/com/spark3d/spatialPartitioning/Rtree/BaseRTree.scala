package com.spark3d.spatialPartitioning.Rtree


import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D

import scala.math._
import scala.collection.mutable.ListBuffer

class BaseRTree (private val maxN: Int){

  def this() {
    this(10)
  }


  private var objectList: List[BoxEnvelope] = _
  private var root: NonLeafNode = _
  private var built: Boolean = false
  private var maxNodeCapacity: Int = 10

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
      buildUpperRTree(objectList, -1)
    }
  }

  private def buildUpperRTree(levelNodes: List[AnyRef], level: Int): NonLeafNode = {

    val levelParents = constructParents(levelNodes, level+1)

    if (levelParents.size == 1) {
      return levelParents(0).asInstanceOf[NonLeafNode]
    }

    buildUpperRTree(levelParents, level+1)
  }

  private def constructParents(children: List[AnyRef], level: Int): List[AnyRef] = {

//    assert(children.foreach(x => x.isInstanceOf[Shape3D]))
    var parents = ListBuffer[AnyRef]()

    parents += new NonLeafNode(level)

    val minLeafCount = ceil(children.size / maxNodeCapacity).toInt

    val sortedChildren = children.sortWith((x, y) =>
        x.asInstanceOf[Shape3D].center.x < y.asInstanceOf[Shape3D].center.x)

    val parentSlices = verticalSlices(sortedChildren, ceil(sqrt(minLeafCount)).toInt)
    createParentFromChildSlices(parentSlices, level)

  }

  def createParentFromChildSlices(parentSlices: List[List[AnyRef]], level: Int): List[AnyRef] = {
    val parents = ListBuffer[AnyRef]()
    for (i <- parentSlices) {
      parents += createParentFromSlices(i, level)
    }
    parents.toList
  }

  def createParentFromSlices(parentSlices: List[AnyRef], level: Int): List[AnyRef] = {
    val parents = ListBuffer[NonLeafNode]()
    parents += new NonLeafNode(level)
    val it = parentSlices.iterator
    while (it.hasNext) {
      val parent = it.next.asInstanceOf[Node]
      if (parents.last.children.size == maxNodeCapacity) {
        parents += new NonLeafNode(level)
      }
      parents.last.children += parent
    }
    parents.toList
  }

  def verticalSlices(children: List[AnyRef], sliceCount: Int): List[List[AnyRef]] = {
    val sliceCapacity = (ceil(children.size) / sliceCount.asInstanceOf[Double]).asInstanceOf[Int]
    val it = children.iterator
    val slices = new Array[ListBuffer[AnyRef]](sliceCount)
    for (i <- 0 to sliceCount) {
      slices(i) = ListBuffer[AnyRef]()
      var added = 0
      while (it.hasNext && added < sliceCapacity) {
        val temp = it.next
        slices(i) += temp
        added += 1
      }
    }

    slices.toList.map(x => x.toList).toList
  }

  def getLeafNodes(): List[AnyRef] = {
    val leafNodes = ListBuffer[AnyRef]()
    getLeafNodes(root, leafNodes)
    leafNodes.toList
  }

  def getLeafNodes(node: Node, leafNodes: ListBuffer[AnyRef]): Unit = {

    val leafNodes = ListBuffer[AnyRef]()

    var isLeaf = false

    root.children.map(x => {
      if (x.isInstanceOf[LeafNode]){
        isLeaf = true
      }
    })

    if (isLeaf) {
      leafNodes += root.envelope
      return
    } else {
      for (child <- root.children) {
        getLeafNodes(child, leafNodes)
      }
    }
  }
}
