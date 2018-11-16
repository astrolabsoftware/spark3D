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
package com.astrolabsoftware

import org.apache.spark.sql.DataFrame
import org.apache.spark.Partitioner
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

import org.apache.spark.sql.SparkSession

import com.astrolabsoftware.spark3d.spatialPartitioning.SpatialPartitioner
import com.astrolabsoftware.spark3d.Repartitioning

package object spark3d {

  /**
    * Set of implicit methods for DataFrame
    */
  implicit class DFExtended(df : DataFrame) {

    /**
      * Add a new method {{partitionBy}} to DataFrame.
      * This method allows to use a custom partitioner (SpatialPartitioner).
      * {{options}} must contain four entries:
      *   - gridtype: the type of repartitioning. Available: onion, octree.
      *   - geometry: geometry of objects: points, spheres, or boxes
      *   - coordSys: coordinate system: spherical or cartesian
      *   - colnames: comma-separated names of the spatial coordinates. For points,
      *   must be "x,y,z" or "r,theta,phi". For spheres, must be "x,y,z,R" or
      *   "r,theta,phi,R".
      *
      * See Repartitioning.partitionBy for the implementation.
      *
      * @param options : Map[String, String] containing metadata (see above).
      * @return (Int) The key of the partition as Int.
      *
      */
    def partitionBy(options: Map[String, String], numPartitions : Int = -1) : DataFrame = {
      Repartitioning.partitionBy(df, options, numPartitions)
    }
  }
}
