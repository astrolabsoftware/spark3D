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
package com.astrolabsoftware.spark3d.spatialPartitioning.RTree

import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.spatialPartitioning.Rtree._

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.mutable.ListBuffer

class RtreeTest extends FunSuite with BeforeAndAfterAll {

  var tree_space: BoxEnvelope = _
  var valid_tree: BaseRTree = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    valid_tree = new BaseRTree(2)
  }

  def containsElement(list: List[BoxEnvelope], obj: BoxEnvelope): Boolean = {
    val objStr = obj.toString

    for (element <- list) {
      if (element.toString == objStr) {
        return true
      }
    }
    false
  }

  test ("Can you insert the elements into a RTree and verify its correctness?") {
    val element1 = BoxEnvelope.apply(0.0, 0.9, 0.0, 0.9, 0.0, 0.9)
    val element2 = BoxEnvelope.apply(1.0, 2.9, 1.0, 2.9, 1.0, 2.9)
    val element3 = BoxEnvelope.apply(1.0, 1.9, 1.0, 1.9, 1.0, 1.9)
    val element4 = BoxEnvelope.apply(0.0, 0.9, 1.0, 1.9, 0.0, 0.9)
    val elements = List(element1, element2, element3, element4)
    valid_tree.insert(elements)
    valid_tree.build

    assert(valid_tree.root != null)
    // maxNodeCapacity is 2 and there are 4 input elements. So a 2 level tree should
    // be created with each leaf node having 2 elements
    assert(valid_tree.root.children.size == 2)

    assert(valid_tree.root.children(0).children.size == 2)
    assert(valid_tree.root.children(1).children.size == 2)

    val leaf_node_set_0 = valid_tree.root.children(0).children.map(x => x.envelope).toList
    val leaf_node_set_1 = valid_tree.root.children(1).children.map(x => x.envelope).toList

    assert(containsElement(leaf_node_set_0, element1))
    assert(containsElement(leaf_node_set_0, element4))

    assert(containsElement(leaf_node_set_1, element2))
    assert(containsElement(leaf_node_set_1, element3))


    val leaves = valid_tree.getGrids
    assert(leaves.size == 2)

  }
}
