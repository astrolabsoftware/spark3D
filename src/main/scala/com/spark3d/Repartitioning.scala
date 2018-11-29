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
package com.astrolabsoftware.spark3d

import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.spark_partition_id


import scala.collection.mutable.{HashSet, ListBuffer}

import com.astrolabsoftware.spark3d.Partitioners
import com.astrolabsoftware.spark3d.spatialPartitioning.KeyPartitioner
import com.astrolabsoftware.spark3d.geometryObjects.Point3D

/**
  * Main object containing methods to repartition DataFrames.
  * Unlike RDDs, Apache Spark Dataset & DataFrame cannot use custom partitioner.
  * This work is an attempt to bridge the gap and allow custom repartitioning of DataFrame.
  *
  * Note that there exists some methods to repartition DataFrame, but they are not
  * deterministic (i.e. they use sampling). See SPARK-26024 for example.
  */
object Repartitioning {

  /**
    * Add a DataFrame column describing the partitioning. This method allows to use a custom
    * partitioner (SpatialPartitioner). Note that no data movement (shuffle) is performed yet here,
    * as we just describe how the repartitioning should be done. Use `repartitionByCol` to
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
    * @param df : Input DataFrame
    * @param options : Map[String, String] containing metadata (see above).
    * @param numPartitions : (optional) The number of partitions wanted. -1 by default,
    *   i.e. the number of partitions of the input DF.
    * @return Input DataFrame plus an additional column `partition_id`.
    */
  def prePartition(df : DataFrame, options: Map[String, String], numPartitions : Int = -1) : DataFrame = {

    // Change the number of partitions if wanted
    val numOfPartitions = numPartitions match {
      case -1 => df.rdd.getNumPartitions
      case x if x > 0 => numPartitions
      case _ => throw new AssertionError(s"""
        The number of partitions must be strictly greater than zero!
        Otherwise leave it unset to take the number of partitions
        of the input DataFrame.
        (You put: $numPartitions)
        """)
    }

    // Geometry of objects
    val geometry = options("geometry")

    // Branch out: keep current partitioning or apply a custom one.
    val dfout = options("gridtype") match {
      // Return current DF with default partition ID
      case "current" => {
        df.repartition(numOfPartitions).withColumn("partition_id", spark_partition_id())
      }

      // Other implemented repartitioners
      case grid @ ("onion" | "octree") => {
        // Definition of the coordinate system. Spherical or cartesian
        val isSpherical : Boolean = options("coordSys") match {
          case "spherical" => true
          case "cartesian" => false
          case _ => throw new AssertionError("""
            Coordinate system not understood! You must choose between:
            spherical, cartesian
            """)
        }

        // Column names must be comma-separated.
        val colnames : Array[String] = options("colnames").split(",")
        val colIndex = colnames.map(x => df.columns.indexOf(x))

        // Assume inputs have the same type
        val inputType = df.dtypes(colIndex(0))._2

        val P = new Partitioners(df, options)
        val partitioner = P.get(numOfPartitions)

        // Add a column with the new partition indices
        val dfExt = geometry match {
          case "points" => {
            // UDF for the repartitioning
            // val placePointsUDF = udf[Int, Double, Double, Double, Boolean](partitioner.placePoints)

            def mapElements(iter: Iterator[Row]) : Iterator[(Double, Double, Double, Int)] = {
              val result = ListBuffer[(Double, Double, Double, Int)]()
              while (iter.hasNext) {
                val data = iter.next
                // val p = new Point3D(data._1, data._2, data._3, isSpherical)
                val myRow = inputType match {
                  case "DoubleType" => List(((
                    data.getDouble(colIndex(0)),
                    data.getDouble(colIndex(1)),
                    data.getDouble(colIndex(2)),
                    partitioner.placePoints(
                      data.getDouble(colIndex(0)),
                      data.getDouble(colIndex(1)),
                      data.getDouble(colIndex(2)),
                      isSpherical))))
                  case "FloatType" => List(((
                    data.getFloat(colIndex(0)).toDouble,
                    data.getFloat(colIndex(1)).toDouble,
                    data.getFloat(colIndex(2)).toDouble,
                    partitioner.placePoints(
                      data.getFloat(colIndex(0)).toDouble,
                      data.getFloat(colIndex(1)).toDouble,
                      data.getFloat(colIndex(2)).toDouble,
                      isSpherical))))
                  case "IntegerType" => List(((
                    data.getInt(colIndex(0)).toDouble,
                    data.getInt(colIndex(1)).toDouble,
                    data.getInt(colIndex(2)).toDouble,
                    partitioner.placePoints(
                      data.getInt(colIndex(0)).toDouble,
                      data.getInt(colIndex(1)).toDouble,
                      data.getInt(colIndex(2)).toDouble,
                      isSpherical))))
                }
                result ++= myRow
              }
              result.iterator
            }

            // df.withColumn("partition_id",
            //   placePointsUDF(
            //     col(colnames(0)).cast("double"),
            //     col(colnames(1)).cast("double"),
            //     col(colnames(2)).cast("double"),
            //     lit(isSpherical)
            //   )
            // )
            val locSpark = SparkSession.getActiveSession.get
            import locSpark.implicits._
            df.rdd.mapPartitions(mapElements).toDF(df.columns(0), df.columns(1), df.columns(2), "partition_id")
            // Array(df.columns(0), df.columns(1), df.columns(2), "partition_id")
            //df.schema.add("partition_id", IntegerType)
          }
          case "spheres" => {
            // UDF for the repartitioning
            val placePointsUDF = udf[Int, Double, Double, Double, Double, Boolean](partitioner.placeSpheres)

            df.withColumn("partition_id",
              placePointsUDF(
                col(colnames(0)).cast("double"),
                col(colnames(1)).cast("double"),
                col(colnames(2)).cast("double"),
                col(colnames(3)).cast("double"),
                lit(isSpherical)
              )
            )
          }
          case _ => throw new AssertionError("""
            Geometry not understood! You must choose between:
            points or spheres
            """)
        }
        dfExt
      }
      case _ => throw new AssertionError("""
        Gridtype not understood! You must choose between:
        onion, octree, or current
        """)
    }

    dfout
  }

  /**
    * Repartition a DataFrame according to a column containing explicit ordering.
    * Note this is not re-ordering elements, but making new partitions with objects
    * having the same partition ID defined by one of the DataFrame column (i.e. shuffling).
    *
    * @param df : input DataFrame.
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
    *
    * > dfp = repartitionByCol(df, "partition_id", true)
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
  def repartitionByCol(df: DataFrame, colname: String, preLabeled: Boolean, numPartitions: Int = -1): DataFrame = {

    // Build a Map (k=df.col -> v=partition_id)
    // to allow the use of standard (Int) partitioners (can be costly).
    val mapPart : Map[Any, Int] = preLabeled match {
      case false => df.select(col(colname)).distinct().collect().map(_(0)).zipWithIndex.toMap

      // If already preLabeled, i.e. `colname` already
      // contains partition ID return empty Map
      case true => Map[Any, Int]()
    }

    // Compute the number of partitions if not provided
    // Number of partitions is the number of distinct values in the specified columns.
    val numOfPartitions = numPartitions match {
      case -1 => mapPart.size match {
        // Return mapPart size if already available
        case x if x > 0 => x
        // Otherwise compute it (can be costly)
        case 0 => df.select(col(colname)).distinct().collect().size
      }
      case x if x > 0 => numPartitions
      case _ => throw new AssertionError("""
        The number of partitions must be strictly greater than zero!
        Otherwise leave it unset to take the number of partitions
        of the input DataFrame.
        (You put: $numPartitions)
        """)
     }

    // Where is the column of interest.
    val position = df.columns.indexOf(colname)

    // Simple key partitioner
    val kp = new KeyPartitioner(numOfPartitions)

    // Apply the partitioning in the RDD world
    val rdd = mapPart.size match {
      case x if x > 0 => {
        df.rdd
        .map(x => (mapPart(x(position)), x))
        .partitionBy(kp)
        .values
      }
      case 0 => {
        df.rdd
        .map(x => (x(position), x))
        .partitionBy(kp)
        .values
      }
    }

    // Go back to DF
    SparkSession.getActiveSession.get.createDataFrame(rdd, df.schema)

  }
}
