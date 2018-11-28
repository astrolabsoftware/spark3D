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
package com.astrolabsoftware

import org.apache.spark.sql.DataFrame

import com.astrolabsoftware.spark3d.Repartitioning
import com.astrolabsoftware.spark3d.Checkers

package object spark3d {

  /**
    * Set of implicit methods for DataFrame
    */
  implicit class DFExtended(df : DataFrame) {

    /**
      * Add a DataFrame column describing the partitioning. This method allows to use a custom
      * partitioner (SpatialPartitioner). Note that no data movement (shuffle) is performed here,
      * as we just describe how the repartitioning should be done. Use `partitionBy` to
      * trigger it.
      *
      *`options` must contain four entries:
      *   - gridtype: the type of repartitioning. Available: current (no repartitioning), onion, octree.
      *   - geometry: geometry of objects: points, spheres, or boxes
      *   - coordSys: coordinate system: spherical or cartesian
      *   - colnames: comma-separated names of the spatial coordinates. For points,
      *     must be "x,y,z" or "r,theta,phi". For spheres, must be "x,y,z,R" or
      *     "r,theta,phi,R".
      *
      * @param options : Map[String, String] containing metadata (see above).
      * @param numPartitions : (optional) The number of partitions wanted. -1 by default,
      *   i.e. the code will try to guess something.
      * @return Input DataFrame plus an additional column `partition_id`.
      */
    def prePartition(options: Map[String, String], numPartitions : Int = -1) : DataFrame = {
      Repartitioning.prePartition(df, options, numPartitions)
    }

    /**
      * Repartition a DataFrame according to a column containing explicit ordering.
      * Note this is not re-ordering elements, but making new partitions with objects
      * having the same partition ID defined by one of the DataFrame column (i.e. shuffling).
      *
      * @param colname : Column name describing the repartitioning. Typically Ints.
      * @param preLabeled : Boolean. true means the column containing the partition ID contains
      *   already numbers from 0 to `numPartitions - 1`. false otherwise. Note that in the latter,
      *   the execution time will be longer as we need to map column values to partition ID.
      * @param numPartitions : Optional. Number of partitions. If not provided the code will
      *   guess the number of partitions by counting the number of distinct elements of
      *   the repartitioning column. As it can be costly, you can provide manually this information.
      * @return Repartitioned input DataFrame.
      *
      * In other words, the column used for the partitioning should contain numbers describing
      * the partition indices:
      *
      * > df.show()
      *  +-------------------+-------------------+------------------+
      *  |            Z_COSMO|                 RA|               Dec|
      *  +-------------------+-------------------+------------------+
      *  |   0.54881352186203|    1.2320476770401| 2.320105791091919|
      *  | 0.7151893377304077|0.12929722666740417|1.3278003931045532|
      *  | 0.6027633547782898|  2.900634288787842| 2.996480941772461|
      *  | 0.5448831915855408| 1.2762248516082764|0.5166937112808228|
      *  |0.42365479469299316|  2.966549873352051|1.4932578802108765|
      *  +-------------------+-------------------+------------------+
      *
      * > options = Map(
      *     "geometry" -> "points",
      *     "colnames" -> "Z_COSMO,RA,DEC",
      *     "coordSys" -> "spherical",
      *     "gridtype" -> "LINEARONIONGRID")
      * > val dfExt = df.prePartition(options, 3, true)
      * > dfExt.show()
      *  +-------------------+-------------------+------------------+------------+
      *  |            Z_COSMO|                 RA|               Dec|partition_id|
      *  +-------------------+-------------------+------------------+------------+
      *  |   0.54881352186203|    1.2320476770401| 2.320105791091919|           0|
      *  | 0.7151893377304077|0.12929722666740417|1.3278003931045532|           1|
      *  | 0.6027633547782898|  2.900634288787842| 2.996480941772461|           0|
      *  | 0.5448831915855408| 1.2762248516082764|0.5166937112808228|           0|
      *  |0.42365479469299316|  2.966549873352051|1.4932578802108765|           2|
      *  +-------------------+-------------------+------------------+------------+
      *
      * will be repartitioned according to partition_id in 3 partitions (0, 1, 2) as
      * > val dfp = dfExt.repartitionByCol("partition_id")
      * > dfp.show()
      *  +-------------------+-------------------+------------------+------------+
      *  |            Z_COSMO|                 RA|               Dec|partition_id|
      *  +-------------------+-------------------+------------------+------------+
      *  |   0.54881352186203|    1.2320476770401| 2.320105791091919|           0|
      *  | 0.6027633547782898|  2.900634288787842| 2.996480941772461|           0|
      *  | 0.5448831915855408| 1.2762248516082764|0.5166937112808228|           0|
      *  | 0.7151893377304077|0.12929722666740417|1.3278003931045532|           1|
      *  |0.42365479469299316|  2.966549873352051|1.4932578802108765|           2|
      *  +-------------------+-------------------+------------------+------------+
      */
    def repartitionByCol(colname: String, preLabeled: Boolean, numPartitions: Int = -1): DataFrame = {
      Repartitioning.repartitionByCol(df, colname, preLabeled, numPartitions)
    }

    /**
      * DataFrame containing the weight of each partition.
      * You can choose between outputing the size (number of rows) of each partition
      * or the fractional size (%) to the total number of rows.
      * size of the dataset (in percent). This is useful to check whether the
      * load is correctly balanced.
      *
      * @param kind : print the load balancing in terms of fractional size (kind="frac")
      *   or number of rows per partition (kind="size"). Default is "frac".
      * @param numberOfElements : Long (optional). Total number of elements in the DataFrame.
      *   Only needed if you choose to output fractional sizes (kind="frac").
      *   If not provided (i.e. default value of -1) and kind="frac", it will be computed (count).
      * @return DataFrame containing the weight of each partition.
      */
    def checkLoadBalancing(kind: String = "frac", numberOfElements: Long = -1L): DataFrame = {
      Checkers.checkLoadBalancing(df, kind, numberOfElements)
    }
  }
}
