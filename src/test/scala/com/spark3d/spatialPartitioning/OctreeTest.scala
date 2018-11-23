/*
 * Copyright 2018 AstroLab Software
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
package com.astrolabsoftware.spark3d.spatialPartitioning

import com.astrolabsoftware.spark3d.geometryObjects._

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.mutable.ListBuffer

class OctreeTest extends FunSuite with BeforeAndAfterAll {

  var tree_space: BoxEnvelope = _
  var valid_tree: Octree = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    tree_space = BoxEnvelope.apply(0.0, 4.0, 0.0, 4.0, 0.0, 4.0)
    valid_tree = new Octree(new BoxEnvelope(tree_space), 0, null,2)
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
    val element1 = BoxEnvelope.apply(0.0, 0.9, 0.0, 0.9, 0.0, 0.9)
    val element2 = BoxEnvelope.apply(1.0, 2.9, 1.0, 2.9, 1.0, 2.9)
    valid_tree.insertElement(element1)
    valid_tree.insertElement(element2)

    val spr = new ShellEnvelope(1.0, 1.0, 1.0, false, 0.9)
    var result = valid_tree.getElements(spr)
    assert(valid_tree.isLeaf)
    assert(result.size == 2)
    assert(containsElement(result, element1))
    assert(containsElement(result, element2))
    var leafNodes = valid_tree.getLeafNodes
    assert(leafNodes.size == 1)
    assert(containsElement(leafNodes, tree_space))

    val element3 = BoxEnvelope.apply(1.0, 1.9, 1.0, 1.9, 1.0, 1.9)
    valid_tree.insertElement(element3)
    result = valid_tree.getElements(BoxEnvelope.apply(3.0, 3.9, 3.0, 3.9, 3.0, 3.9))
    assert(result.size == 1)
    assert(containsElement(result, element2))
    result = valid_tree.getElements(element1)
    assert(result.size == 3)
    assert(containsElement(result, element1))
    assert(containsElement(result, element2))
    assert(containsElement(result, element3))

    val element4 = BoxEnvelope.apply(0.0, 0.9, 1.0, 1.9, 0.0, 0.9)
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
    valid_tree = new Octree(new BoxEnvelope(tree_space), 0, null, 2)
    valid_tree.insertElement(new ShellEnvelope(1.0, 1.0, 1.0, false, 1.0))
    valid_tree.forceGrowTree(2)
    assert(valid_tree.getLeafNodes.size == 64)
  }
}
