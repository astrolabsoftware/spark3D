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
package com.astrolabsoftware.spark3d.utils

import java.io.Serializable

import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D

import scala.collection.generic.Growable
import scala.collection.mutable

import collection.mutable.PriorityQueue

/**
  * A wrapper on top of the normal priority queue, so that it is bounded by a given capacity
  * (in case of overflow, replace the element based on the priority defined) and at the same time
  * ensure that the priority queue contains only unique elements.
  *
  * @param maxSize
  * @param ord
  * @tparam A
  */
class BoundedUniquePriorityQueue[A <: Shape3D](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  // underlying base priority queue
  private val underlying = new PriorityQueue[A]()(ord)

  // HashSet of elements contained in the priority queue used to ensure uniqueness of the elements
  // in the priority queue.
  private val containedElements = new mutable.HashSet[Int]()

  override def iterator: Iterator[A] = underlying.iterator

  override def size: Int = underlying.size

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem: A): this.type = {
    val elementHash = elem.center.getCoordinate.hashCode
    // check if element to be inserted is unique or not
    if (!containedElements.contains(elementHash)) {
      if (size < maxSize) {
        underlying.enqueue(elem)
        containedElements.add(elementHash)
      } else {
        maybeReplaceLowest(elem)
      }
    }

    this
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear() { underlying.clear() }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.head
    // Definition of scala.Ordering.get(x, y) is:
    // Returns true iff y comes before x in the ordering and is not the same as x.
    if (head != null && ord.gt(head, a)) {
      underlying.dequeue
      underlying.enqueue(a)
      containedElements.add(a.center.getCoordinate.hashCode)
      containedElements.remove(head.center.getCoordinate.hashCode)
      true
    } else {
      false
    }
  }
}
