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
package com.astrolabsoftware.spark3d.utils

import java.io.Serializable

import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D

import scala.collection.generic.Growable
import collection.mutable.PriorityQueue

class BoundedUniquePriorityQueue[A <: Shape3D](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  private val underlying = new PriorityQueue[A]()(ord)

  override def iterator: Iterator[A] = underlying.iterator

  override def size: Int = underlying.size

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem: A): this.type = {
    if (!underlying.map(x => x.center).toList.contains(elem.center)) {
      if (size < maxSize) {
        underlying.enqueue(elem)
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
    if (head != null && ord.gt(a, head)) {
      underlying.dequeue
      underlying.enqueue(a)
      true
    } else {
      false
    }
  }
}

