/*
 * Copyright 2018 Mayur Bhosale
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spark3d.spatialPartitioning

import com.spark3d.geometryObjects._
import com.spark3d.geometryObjects.Shape3D.Shape3D

import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue

/**
  * Octree is a 3D extension of Quadtree where in at each stage node (Cuboid)
  * (instead of rectangle in Quadtree case) is split into 8 sub nodes.
  *  Each node can contain
  * It is assumed that one can place an object only using its bounding box, a BoxEnvelope
  *
  * @param box root node of this octree
  * @param maxItemsPerNode maximum number of items per Cuboid (box)
  */

/**
  * Octree is a 3D extension of Quadtree where in at each stage node (Cuboid)
  * (instead of rectangle in Quadtree case) is split into 8 sub nodes.
  *
  * @param box BoxEnvelope (boundary) of the tree rooted at this node
  * @param level The depth of the node compared to the root of original tree
  * @param maxItemsPerNode maximum number of elements per node
  * @param maxLevel maximum level upto which tree can grow
  */
class Octree(
    val box: BoxEnvelope,
    val level: Int,
    val parentNode: Octree = null,
    val maxItemsPerNode: Int = 5,
    val maxLevel: Int = 10)
  extends Serializable {

  // list of elements inside this node
  private final val elements = new ListBuffer[BoxEnvelope]
  // number of elements currently in the node
  private var numElements = 0
  // the array of children nodes
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
    * split into 8 sub-nodes (Cuboids)
    */
  private def splitBox(): Unit = {
    children = new Array[Octree](8)

    children(CHILD_L_SW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        box.minY, (box.maxY - box.minY) / 2,
        box.minZ, (box.maxZ - box.minZ) / 2),
      level + 1, this, maxItemsPerNode, maxLevel)

    children(CHILD_L_SE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        box.minY, (box.maxY - box.minY) / 2,
        box.minZ, (box.maxZ - box.minZ) / 2),
      level + 1, this, maxItemsPerNode, maxLevel)

    children(CHILD_L_NW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        (box.maxY - box.minY) / 2, box.maxY,
        box.minZ, (box.maxZ - box.minZ) / 2),
      level + 1, this, maxItemsPerNode, maxLevel)

    children(CHILD_L_NE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        (box.maxY - box.minY) / 2, box.maxY,
        box.minZ, (box.maxZ - box.minZ) / 2),
      level + 1, this, maxItemsPerNode, maxLevel)

    children(CHILD_U_SW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        box.minY, (box.maxY - box.minY) / 2,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      level + 1, this, maxItemsPerNode, maxLevel)

    children(CHILD_U_SE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        box.minY, (box.maxY - box.minY) / 2,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      level + 1, this, maxItemsPerNode, maxLevel)

    children(CHILD_U_NW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        (box.maxY - box.minY) / 2, box.maxY,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      level + 1, this, maxItemsPerNode, maxLevel)

    children(CHILD_U_NE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        (box.maxY - box.minY) / 2, box.maxY,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      level + 1, this, maxItemsPerNode, maxLevel)

  }

  /**
    * Finds the region in which the input objects belongs.
    * A node is split into 8 children boxes when it has contained the objects to its maximum
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

  /**
    * Insert an input object into the Octree
    *
    * @param obj Shape3D object to be inserted
    */
  def insertElement(obj: Shape3D): Unit = {
    insertElement(obj.getEnvelope)
  }

  /**
    * Insert an input object defined by its bounding box into the octree
    *
    * @param element
    */
  def insertElement(element: BoxEnvelope): Unit = {

    // find the region of the element
    val region = findRegion(element, true)

    // if the object belongs to this node insert it and return
    // (this also means this node is not filled up to the capacity as of now.)
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
      // insert these elements into children nodes. If a elements doesn't fit in any of the child
      // nodes, it will be placed in each of the child node as it it placed in their parent
      for (element <- tempElements) {
        insertElement(element)
      }
    }
  }

  /**
    * Perform a Breadth First Search Traversal (BFS) of the tree.
    *
    * @param func anonymous function to decide if the desired action should be performed on the this
    *             node or not
    * @param data a ListBuffer in which the desired data should be placed when the funct() == true
    * @param actionID -2 => collect data in the all nodes
    *                 -1 => get the bounding box of the node
    *                 x, where x > 0 => assign partitionID to the this node
    */
  private def bfsTraverse(func: Octree => Boolean, data: ListBuffer[BoxEnvelope],
                          actionID: Int): Unit = {

    // create a queue
    val que = new Queue[Octree]
    // insert the root node
    que += this
    var partitionID = actionID
    while (!que.isEmpty) {
      val current = que.dequeue
      if (func(current)) {
        if (actionID == -2) {
          // get all the elements in the tree
          data ++= current.elements
        } else if (actionID == -1) {
          // add this to the leaf node
          data += current.box
        } else {
          // assign the partitionID for this node
          current.box.indexID = partitionID
          partitionID += 1
        }
      }

      if (!current.isLeaf) {
        // add children to the queue
        for (child <- current.children) {
          que += child
        }
      }
    }
  }

  /**
    * Perform a Depth First Search Traversal (DFS) of the tree.
    *
    * @param func anonymous function to decide if the desired action should be performed on the this
    *             node or not
    * @param obj input object for which the search is to be performed
    * @param data a ListBuffer in which the desired data should be placed when the funct() == true
    */
  private def dfsTraverse(func: (Octree, BoxEnvelope) => Boolean, obj: BoxEnvelope, data: ListBuffer[Octree]): Unit = {
    if (func(this, obj)) {
      data += this
    }

    if (!isLeaf) {
      for (child <- children) {
        child.dfsTraverse(func, obj, data)
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

  /**
    * Get all the elements contained by the input Shape3D's bounding box.
    *
    * @param element bounding box of the Shape3D for which the elements search is to be performed
    * @return list of elements contained by the bounding box of the Shape3D
    */
  def getElements(element: BoxEnvelope): ListBuffer[BoxEnvelope] = {

    val region = findRegion(element, false)

    var containedElements = new ListBuffer[BoxEnvelope]

    if (region != SELF_NODE) {
      // add the elements in this node. If the object is placed in the parent even after
      // it is split into children (when it is too big to be put in any of the child node), it
      // is considered to be part of the all child nodes
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
    * Check if this node is a leaf node or not based its children
    * @return true if its a leaf node, false otherwise
    */
  def isLeaf(): Boolean = {
    children == null
  }


  /** Forcefully make the tree grow till the level specified
    *
    * @param level depth upto which the tree is to be grown
    */
  def forceGrowTree(level: Int): Unit = {
    val currentTree = this
    if (level > 0) {
      if (currentTree.isLeaf) {
        currentTree.splitBox
      }
      for (child <- children) {
        child.forceGrowTree(level-1)
      }
    }
  }

  /**
    * Assigns the partition ID to each of the leaf node.
    */
  def assignPartitionIDs(): Unit = {
    val traverseFunct: Octree => Boolean = {
      node => node.isLeaf
    }
    bfsTraverse(traverseFunct, null, 0)
  }

  /**
    * get all the containing Envelopes of the leaf nodes, which intersect, contain or are contained
    * by the input BoxEnvelope
    *
    * @param obj Input object to be checked for the match
    * @return list of Envelopes of the leafNodes which match the conditions
    */
  def getMatchedLeafBoxes(obj: BoxEnvelope): ListBuffer[BoxEnvelope] = {

    val matchedLeaves = getMatchedLeaves(obj)
    matchedLeaves.map(x => x.box)
  }

  /**
    * get all the containing Envelopes of the leaf nodes, which intersect, contain or are contained
    * by the input BoxEnvelope
    *
    * @param obj Input object to be checked for the match
    * @return list of leafNodes which match the conditions
    */
  def getMatchedLeaves(obj: BoxEnvelope): ListBuffer[Octree] = {
    val matchedLeaves = new ListBuffer[Octree]
    val traverseFunct: (Octree, BoxEnvelope) => Boolean = {
      (node, obj) => node.isLeaf && (node.box.intersects(obj) ||
        node.box.contains(obj) ||
        obj.contains(node.box))
    }

    dfsTraverse(traverseFunct, obj, matchedLeaves)
    matchedLeaves
  }

  /**
    * Get the neighbors of this node. Neighbors here are leaf sibling or leaf descendants of the
    * siblings.
    *
    * @param queryNode the box of the the input node to avoid passing same node as neighbor
    * @return list of lead neghbors and their index/partition ID's
    */
  def getLeafNeighbors(queryNode: BoxEnvelope): List[Tuple2[Int, BoxEnvelope]] = {
    val leafNeighbors = new ListBuffer[Tuple2[Int, BoxEnvelope]]
    if (parentNode != null){
      for (neighbor <- parentNode.children) {
        if (!neighbor.box.isEqual(queryNode)) {
          if (neighbor.isLeaf) {
            leafNeighbors += new Tuple2(neighbor.box.indexID, neighbor.box)
          } else {
            leafNeighbors ++= neighbor.children(0).getLeafNeighbors(queryNode)
          }
        }
      }
    }
    leafNeighbors.toList.distinct
  }
}
