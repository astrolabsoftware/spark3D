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

// Spark built-in partitioner
import org.apache.spark.Partitioner

/**
  * Simple Key partitioner (maybe it exists already in Apache Spark?).
  * Produces as many partitions as different keys.
  *
  */
class KeyPartitioner(numOfPartitions: Int) extends Partitioner with Serializable {

  /**
    * Return the total number of partitions in a RDD
    *
    * @return (Int) the number of partitions
    */
  override def numPartitions : Int = {
    numOfPartitions
  }

  /**
    * Method to return the index of a partition
    *
    * @param key : the key of the partition (from key/value)
    * @return (Int) the key of the partition as Int.
    */
  override def getPartition(key : Any) : Int = {
    key match {
      case i:Int => key.asInstanceOf[Int]
      case l:Long => key.asInstanceOf[Long].toInt
      case _ => throw new ClassCastException("""
        Key from KeyPartitioner must be Int or Long!
        """)
    }
  }
}
