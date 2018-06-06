package com.spark3d.spatialPartitioning

import com.spark3d.geometry.BoxEnvelope
import com.spark3d.geometryObjects._

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.mutable.ListBuffer

class OctreeTest extends FunSuite with BeforeAndAfterAll {

  var tree_space: BoxEnvelope = _
  var valid_tree: Octree = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    tree_space = BoxEnvelope.apply(0.0, 4.0, 0.0, 4.0, 0.0, 4.0)
    valid_tree = new Octree(new BoxEnvelope(tree_space), 0, 2)
  }

  def containsElement(list: ListBuffer[BoxEnvelope], obj: BoxEnvelope): Boolean = {
    val objStr = obj.toString

    for (element <- list) {
      if (element.toString == objStr) {
        return true
      }
    }
    false
  }

  test ("Can you insert the elements into a Octree and verify its correctness?") {
    val element1 = BoxEnvelope.apply(0.0, 1.0, 0.0, 1.0, 0.0, 1.0)
    val element2 = BoxEnvelope.apply(1.0, 3.0, 1.0, 3.0, 1.0, 3.0)
    valid_tree.insertElement(element1)
    valid_tree.insertElement(element2)

    val spr = new Sphere(1.0, 1.0, 1.0, 1.0)
    var result = valid_tree.getElements(spr)
    assert(valid_tree.isLeaf)
    assert(result.size == 2)
    assert(containsElement(result, element1))
    assert(containsElement(result, element2))
    var leafNodes = valid_tree.getLeafNodes
    assert(leafNodes.size == 1)
    assert(containsElement(leafNodes, tree_space))

    val element3 = BoxEnvelope.apply(1.0, 2.0, 1.0, 2.0, 1.0, 2.0)
    valid_tree.insertElement(element3)
    result = valid_tree.getElements(BoxEnvelope.apply(3.0, 4.0, 3.0, 4.0, 3.0, 4.0))
    assert(result.size == 1)
    assert(containsElement(result, element2))
    result = valid_tree.getElements(element1)
    assert(result.size == 3)
    assert(containsElement(result, element1))
    assert(containsElement(result, element2))
    assert(containsElement(result, element3))

    val element4 = BoxEnvelope.apply(0.0, 1.0, 1.0, 2.0, 0.0, 1.0)
    valid_tree.insertElement(element4)

    result = valid_tree.getElements(element1)
    assert(!valid_tree.isLeaf)
    assert(result.size == 2)
    assert(containsElement(result, element1))
    assert(containsElement(result, element2))
    leafNodes = valid_tree.getLeafNodes
    assert(leafNodes.size == 15)

    valid_tree.assignPartitionIDs
    leafNodes = valid_tree.getLeafNodes
    for (node <- leafNodes) {
      assert(node.indexID <= 14 && node.indexID >= 0)
    }
  }

  test ("Can you force grow the octree to given level?") {
    valid_tree = new Octree(new BoxEnvelope(tree_space), 0, 2)
    valid_tree.insertElement(new Sphere(1.0, 1.0, 1.0, 1.0))
    valid_tree.forceGrowTree(2)
    assert(valid_tree.getLeafNodes.size == 64)
  }
}
