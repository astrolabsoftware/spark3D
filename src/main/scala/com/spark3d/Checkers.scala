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
package com.astrolabsoftware.spark3d

import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id

object Checkers {

  def returnFracSize(part: Iterator[Row], numberOfElements: Long): Iterator[Double] = {
    // Number of elements in the partition
    val sizePartition = part.size

    // Use Double
    val frac : Double = sizePartition.toDouble / numberOfElements.toDouble * 100

    // Return an Iterator
    Iterator(frac)
  }

  def returnSize(part: Iterator[Row]): Iterator[Double] = {
    // Return an Iterator
    Iterator(part.size)
  }

  /**
    * DataFrame containing the weight of each partition.
    * You can choose between outputing the size (number of rows) of each partition
    * or the fractional size (%) to the total number of rows.
    * size of the dataset (in percent). This is useful to check whether the
    * load is correctly balanced.
    *
    * @param df : Input DataFrame
    * @param kind : print the load balancing in terms of fractional size (kind="frac")
    *   or number of rows per partition (kind="size"). Default is "frac".
    * @param numberOfElements : Long (optional). Total number of elements in the DataFrame.
    *   Only needed if you choose to output fractional sizes (kind="frac").
    *   If not provided (i.e. default value of -1) and kind="frac", it will be computed (count).
    * @return DataFrame containing the weight of each partition.
    */
  def checkLoadBalancing(df: DataFrame, kind: String = "frac", numberOfElements: Long = -1L) : DataFrame = {

    // Need to import implicits to use toDF method
    val spark2 = SparkSession.getActiveSession.get
    import spark2.implicits._

    // Total number of elements in the DF.
    val numberOfElementsPriv: Long = numberOfElements match {
      case -1 => {
        kind match {
          case "frac" => df.count()
          // If not kind="frac", we do not need to total number of rows.
          case _ => -1L
        }
      }
      case x if x > 0 => numberOfElements
      case _ => throw new AssertionError("""
        Total number of elements in the DataFrame must be Long greater than 0!
        If you do not know it, set it to -1, and we will compute it for you.
      """)
    }

    // Output a DataFrame containing detail of the load balancing.
    val dfout = kind match {
      case "frac" => df.rdd.mapPartitions(part => returnFracSize(part, numberOfElementsPriv)).toDF("Load (%)")
      case "size" => df.rdd.mapPartitions(returnSize).toDF("Load (#Rows)")
      case _ => throw new AssertionError("""
        Wrong value for `kind`! You must choose between
          - "frac": Output a DataFrame containing the size of each partition
            relative to the total size of the dataset (in percent).
          - "size": Output a DataFrame containing the size of each partition
            in terms of number of rows.
        """)
    }

    dfout.withColumn("partition_id", spark_partition_id())
  }
}
