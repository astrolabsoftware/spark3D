// /*
//  * Copyright 2018 Mayur Bhosale
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// package com.astrolabsoftware.spark3d.spatial3DRDD
//
// import java.util.HashMap
//
// import com.astrolabsoftware.spark3d.geometryObjects._
// import com.astrolabsoftware.spark3d.spatial3DRDD.Loader._
// import com.astrolabsoftware.spark3d.spatialPartitioning.SpatialPartitioner
//
// import org.apache.spark.storage.StorageLevel
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.functions.col
// import org.apache.spark.rdd.RDD
//
//
// class SphereRDD(rdd : RDD[ShellEnvelope],
//     override val isSpherical: Boolean, storageLevel: StorageLevel) extends Shape3DRDD[ShellEnvelope] {
//
//   /**
//     * Construct a RDD[ShellEnvelope] from whatever data source registered in Spark.
//     * For more information about available official connectors:
//     * `https://spark-packages.org/?q=tags%3A%22Data%20Sources%22`
//     *
//     * We currently include: CSV, JSON, TXT, FITS, ROOT, HDF5, Avro, Parquet...
//     *
//     * {{{
//     *   // Here is an example with a CSV file containing
//     *   // 3 cartesian coordinates + 1 radius columns labeled x,y,z,radius.
//     *
//     *   // Filename
//     *   val fn = "path/to/file.csv"
//     *   // Spark datasource
//     *   val format = "csv"
//     *   // Options to pass to the DataFrameReader - optional
//     *   val options = Map("header" -> "true")
//     *
//     *   // Load the data as RDD[ShellEnvelope]
//     *   val rdd = new SphereRDD(spark, fn, "x,y,z,radius", true, format, options)
//     * }}}
//     *
//     * @param spark : (SparkSession)
//     *   The spark session
//     * @param filename : (String)
//     *   File name where the data is stored. Extension must be explicitly
//     *   written (.cvs, .json, or .txt)
//     * @param colnames : (String)
//     *   Comma-separated names of (x, y, z, r) columns to read.
//     *   Example: "Z_COSMO,RA,Dec,Radius".
//     * @param isSpherical : (Boolean)
//     *   If true, it assumes that the coordinates of the center of
//     *   the ShellEnvelope are (r, theta, phi).
//     *   Otherwise, it assumes cartesian coordinates (x, y, z). Default is false.
//     * @param format : (String)
//     *   The name of the data source as registered in Spark. For example:
//     *     - text
//     *     - csv
//     *     - json
//     *     - com.astrolabsoftware.sparkfits
//     *     - org.dianahep.sparkroot
//     *     - gov.llnl.spark.hdf or hdf5
//     * @param options : (Map[String, String])
//     *   Options to pass to the DataFrameReader. Default is no options.
//     * @param storageLevel : (StorageLevel)
//     *   Storage level for the raw RDD (unpartitioned). Default is StorageLevel.NONE.
//     *   See https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence
//     *   for more information.
//     * @return (RDD[ShellEnvelope])
//     *
//     */
//   def this(spark : SparkSession, filename : String,
//       colnames : String, isSpherical: Boolean,
//       format: String, options: Map[String, String] = Map("" -> ""),
//       storageLevel: StorageLevel = StorageLevel.NONE) {
//     this(SphereRDDFromV2(spark, filename, colnames, isSpherical, format, options),
//       isSpherical, storageLevel
//     )
//   }
//
//   /**
//     * Constructor of `SphereRDD` which is suitable for py4j.
//     * It calls `SphereRDDFromV2PythonHelper` instead of `SphereRDDFromV2`.
//     * All args are the same but `options` which is a `java.util.HashMap`, and
//     * `storageLevel` which is removed and set to StorageLevel.NONE automatically
//     * User cannot set the storage level in pyspark3d when loading for the moment,
//     * but it can be done afterwards.
//     *
//     */
//   def this(spark : SparkSession, filename : String, colnames : String, isSpherical: Boolean,
//       format: String, options: HashMap[String, String]) {
//     this(
//       SphereRDDFromV2PythonHelper(
//         spark, filename, colnames, isSpherical, format, options
//       ), isSpherical, StorageLevel.NONE
//     )
//   }
//
//   // Raw partitioned RDD
//   override val rawRDD = rdd
//   rawRDD.persist(storageLevel)
//
//   /**
//     * Constructor of `spatialPartitioning` which is suitable for py4j.
//     * py4j does not handle generics, so we explicitly specify the types here.
//     * See discussion here: https://github.com/bartdag/py4j/issues/328
//     *
//     * Apply any Spatial Partitioner to this.rawRDD[ShellEnvelope],
//     * and return a RDD[ShellEnvelope] with the new partitioning.
//     *
//     * @param partitioner : (SpatialPartitioner)
//     *   Spatial partitioner as defined in utils.GridType
//     * @return (RDD[ShellEnvelope]) RDD whose elements are ShellEnvelope
//     *
//     */
//   def spatialPartitioningPython(partitioner: SpatialPartitioner) : RDD[ShellEnvelope] = {
//     this.partition(partitioner).asInstanceOf[RDD[ShellEnvelope]]
//   }
//
//   /**
//     * Constructor of `spatialPartitioning` which is suitable for py4j.
//     * py4j does not handle generics, so we explicitly specify the types here.
//     * See discussion here: https://github.com/bartdag/py4j/issues/328
//     *
//     * Apply a spatial partitioning to this.rawRDD, and return a RDD[ShellEnvelope]
//     * with the new partitioning.
//     * The list of available partitioning can be found in utils/GridType.
//     * By default, the outgoing level of parallelism is the same as the incoming
//     * one (i.e. same number of partitions).
//     *
//     * @param gridtype : (String)
//     *   Type of partitioning to apply. See utils/GridType.
//     * @param numPartitions : (Int)
//     *   Number of partitions for the partitioned RDD. By default (-1), the
//     *   number of partitions is that of the raw RDD. You can force it to be
//     *   different by setting manually this parameter.
//     *   Be aware of shuffling though...
//     * @return (RDD[ShellEnvelope]) RDD whose elements are ShellEnvelope.
//     *
//     */
//   def spatialPartitioningPython(gridtype: String, numPartitions: Int = -1): RDD[ShellEnvelope] = {
//     this.spatialPartitioning(gridtype, numPartitions).asInstanceOf[RDD[ShellEnvelope]]
//   }
// }
//
// /**
//   * Construct a ShellEnvelopeRDD from a RDD[ShellEnvelope]
//   *
//   * @param rdd : (RDD[ShellEnvelope])
//   *   RDD whose elements are ShellEnvelope instances.
//   * @param isSpherical : (Boolean)
//   *   If true, it assumes that the coordinates of the ShellEnvelope
//   *   center are (r, theta, phi).
//   *   Otherwise, it assumes cartesian coordinates (x, y, z).
//   * @param storageLevel : (StorageLevel)
//   *   Storage level for the raw RDD (unpartitioned). Default is StorageLevel.NONE.
//   *   See https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence
//   *   for more information.
//   *
//   */
// object SphereRDD {
//   def apply(rdd : RDD[ShellEnvelope], isSpherical: Boolean, storageLevel: StorageLevel): SphereRDD = {
//     new SphereRDD(rdd, isSpherical, storageLevel)
//   }
// }
