package com.spark3d.spatialPartitioning

import com.spark3d.geometry._
import com.spark3d.geometryObjects.Shape3D.Shape3D

import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer

/**
  * Octree is a 3D extension of Quadtree where in at each stage node (Cuboid)
  * (instead of rectangle in Quadtree case) is split into 8 sub nodes. As
  * all the sub nodes are contained within the parent node and
  * each level contains increasing the level of resolution of details as
  * we move away from the root level, a search for the data point can
  * be done very easily by moving down the tree along the node
  * which contains some information about a particular point
  * It is assumed that one can place an object only using its bounding box, a BoxEnvelope
  *
  * @param box root node of this octree
  * @param maxItemsPerNode maximum number of items per Cuboid (box)
  */
class Octree(
    val box: BoxEnvelope,
    val level: Int,
    val maxItemsPerNode: Int = 5,
    val maxLevel: Int = 10)
  extends Serializable {

  // list of elements inside this node
  private final val elements = new ListBuffer[BoxEnvelope]
  // number of elements currently in the node
  private var numElements = 0
  // the array of children nodes
// private var children: Array[Octree] = _
  var children: Array[Octree] = _

  val SELF_NODE: Int = -1
  val CHILD_U_NW: Int = 0
  val CHILD_U_NE: Int = 1
  val CHILD_U_SW: Int = 2
  val CHILD_U_SE: Int = 3
  val CHILD_L_NW: Int = 4
  val CHILD_L_NE: Int = 5
  val CHILD_L_SW: Int = 6
  val CHILD_L_SE: Int = 7

  /**
    * Splits this node into 8 children nodes. For this the node (Cuboid) is
    * split into 8 nodes (Cuboids)
    */
  private def splitBox(): Unit = {
    children = new Array[Octree](8)

    children(CHILD_L_SW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        box.minY, (box.maxY - box.minY) / 2,
        box.minZ, (box.maxZ - box.minZ) / 2),
      level + 1, maxItemsPerNode, maxLevel)

    children(CHILD_L_SE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        box.minY, (box.maxY - box.minY) / 2,
        box.minZ, (box.maxZ - box.minZ) / 2),
      level + 1, maxItemsPerNode, maxLevel)

    children(CHILD_L_NW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        (box.maxY - box.minY) / 2, box.maxY,
        box.minZ, (box.maxZ - box.minZ) / 2),
      level + 1, maxItemsPerNode, maxLevel)

    children(CHILD_L_NE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        (box.maxY - box.minY) / 2, box.maxY,
        box.minZ, (box.maxZ - box.minZ) / 2),
      level + 1, maxItemsPerNode, maxLevel)

    children(CHILD_U_SW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        box.minY, (box.maxY - box.minY) / 2,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      level + 1, maxItemsPerNode, maxLevel)

    children(CHILD_U_SE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        box.minY, (box.maxY - box.minY) / 2,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      level + 1, maxItemsPerNode, maxLevel)

    children(CHILD_U_NW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        (box.maxY - box.minY) / 2, box.maxY,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      level + 1, maxItemsPerNode, maxLevel)

    children(CHILD_U_NE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        (box.maxY - box.minY) / 2, box.maxY,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      level + 1, maxItemsPerNode, maxLevel)

  }

  /**
    * Finds the region in which the input objects belongs.
    * A node is split into 8 children boxes when it has contained objects to its maximum
    * capacity specified by the maxItemsPerNode while placing a new object
    *
    * @param obj object for which the placement/placed region is to be found
    * @param split true if the region is to be found for placement
    * @return the region in which the object is to be placed.
    */
   private def findRegion(obj: BoxEnvelope, split: Boolean = true): Int = {
    var region = SELF_NODE

    if (numElements >= maxItemsPerNode && level < maxLevel) {
      if (isLeaf && split) {
        splitBox
      }

      if (!isLeaf) {
        breakable {
          for ((child, i) <- children.view.zipWithIndex) {
            if (child.box.contains(obj)) {
              region = i
              break
            }
          }
        }
      }
    }
    region
  }

  def insertElement(obj: Shape3D): Unit = {
    insertElement(obj.getEnvelope)
  }

  def insertElement(element: BoxEnvelope): Unit = {

    // find the region of the element
    val region = findRegion(element, true)

    //if the object belongs to this node insert it and return
    // (this also means this node is filled up to the capacity as of now.)
    if (region == SELF_NODE || level == maxLevel) {
      elements += element
      numElements += 1
      return
    } else {
      // else insert into a child box where it belongs
      children(region).insertElement(element)
    }

    // handle the case where the number of elements in this node have become more than the capacity
    if (numElements >= maxItemsPerNode && level < maxLevel) {
      val tempElements = elements.clone
      elements.clear
      // and insert these elements into children nodes. If a elements doesn't fit in any of the child
      // nodes, it will be placed in each of the child node as it it placed in their parent now
      for (element <- tempElements) {
        insertElement(element)
      }
    }
  }

  //  def removeElement(element: BoxEnvelope): Unit = {
  //
  //    if (!box.contains(element)) {
  //      throw new AssertionError(
  //        """
  //                Element to be removed must be smaller than the the
  //                space encompassed by this Octree.
  //                """)
  //    }
  //
  //    val region = findRegion(element, false)
  //
  //    if (region == SELF_NODE) {
  //      elements -= element
  //    } else {
  //      children(region).removeElement(element)
  //    }
  //  }
  //

  /**
    * Perform a Breadth First Search Traversal (BFS) of the tree.
    *
    * @param func anonymous function to decide if the desired action should be performed on the this
    *             node or not
    * @param data a ListBuffer in which the desired data should be placed when the funct() == true
    * @param actionID -2 => collect data in the all nodes
    *                 -1 => get the leaf nodes
    *                 x, where x > 0 => assign x as an partitionID to this node
    */
  private def bfsTraverse(func: Octree => Boolean, data: ListBuffer[BoxEnvelope],
                          actionID: Int): Unit = {
    // get data in this node
    var tempActionID = actionID
    if (func(this)) {
      if (tempActionID == -2) {
        // get all the elements in the tree
        data ++= elements
      } else if (tempActionID == -1) {
        // add this to the leaf node
        data += box
      } else {
        // assign actionID as the partitionID for this node
        box.indexID = tempActionID
        tempActionID += 1
      }
    }

    // visit the children node if they exist
    if (!isLeaf) {
      for (child <- children) {
        child.bfsTraverse(func, data, tempActionID)
      }
    }
  }

  /**
    * Get all the elements contained by the input Shape3D. A search is done on the
    * basis of the bounding box of the input Shape3D.
    *
    * @param obj input Shape3D for which the elements search is to be performed
    * @return list of elements contained by the input Shape3D
    */
  def getElements(obj: Shape3D): ListBuffer[BoxEnvelope] = {
    getElements(obj.getEnvelope)
  }

  def getElements(element: BoxEnvelope): ListBuffer[BoxEnvelope] = {

    val region = findRegion(element, false)

    var containedElements = new ListBuffer[BoxEnvelope]

    if (region != SELF_NODE) {
      // add the elements in this node. The object is placed in the parent even after
      // it is split into children is considered to be part of the all child nodes
      containedElements ++= elements
      // add the elements from the child in which the objects actually belong
      containedElements ++= children(region).getElements(element)
    } else {
      // get all the elements in this tree by doing a Breadth First Search (BFS)
      // traversal of the tree
      val traverseFunct: Octree => Boolean = {
        tree => tree != null
      }
      bfsTraverse(traverseFunct, containedElements, -2)
    }

    containedElements
  }

  /**
    * Get all the leaf nodes of the tree.
    *
    * @return a list of the leaf nodes
    */
  def getLeafNodes(): ListBuffer[BoxEnvelope] = {

    val leafNodes = new ListBuffer[BoxEnvelope]
    val traverseFunct: Octree => Boolean = {
      node => node.isLeaf
    }

    bfsTraverse(traverseFunct, leafNodes, -1)
    leafNodes
  }

  /**
    * Check if its a leaf node or not based it has children or not
    * @return true if its a leaf node, false otherwise
    */
  def isLeaf(): Boolean = {
    children == null
  }

  /**
    * Assigns an ID to each of the leaf nodes.
    */
  def assignPartitionIDs(): Unit = {
    val traverseFunct: Octree => Boolean = {
      node => node.isLeaf
    }
    bfsTraverse(traverseFunct, null, 0)
  }
}