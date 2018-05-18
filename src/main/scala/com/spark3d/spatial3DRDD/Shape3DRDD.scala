/*
 * Copyright 2018 Julien Peloton
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
package com.spark3d.spatial3DRDD

import com.spark3d.spatialPartitioning._
import com.spark3d.geometryObjects._
import com.spark3d.geometryObjects.Shape3D._
import com.spark3d.utils.GridType

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.PairFlatMapFunction

class Shape3DRDD[T<:Shape3D] extends Serializable {

  def partition[T<:Shape3D](rdd: RDD[T], partitioner: SpatialPartitioner) : JavaRDD[T] = {
    rdd.toJavaRDD.flatMapToPair(
      new PairFlatMapFunction[T, Int, T]() {
        override def call(spatialObject: T) : java.util.Iterator[Tuple2[Int, T]] = {
          partitioner.placeObject[T](spatialObject)
        }
      }
    ).partitionBy(partitioner).mapPartitions(
      new FlatMapFunction[java.util.Iterator[Tuple2[Int, T]], T]() {
        override def call(tuple2Iterator : java.util.Iterator[Tuple2[Int, T]]) : java.util.Iterator[T] = {
          new java.util.Iterator[T]() {
            override def hasNext() : Boolean = tuple2Iterator.hasNext()

            override def next() : T = tuple2Iterator.next()._2

            override def remove() = throw new UnsupportedOperationException()
          }
        }
      }, true
    )
  }
}
