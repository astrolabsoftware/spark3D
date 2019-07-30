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
package com.astrolabsoftware.spark3d.examples
import scala.math._

// spark3D implicits
import com.astrolabsoftware.spark3d._
// check
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._
import org.apache.spark._ 
import org.apache.spark.SparkConf
import com.astrolabsoftware.spark3d.spatialPartitioning.KDtree
import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope
 
// Spark lib
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._

// Logger
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Main app.
  */
object Test {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // Initialise our spark connector
  val spark = SparkSession
    .builder()
    .appName("partitioning")
    .getOrCreate()

  import spark.implicits._

  /**
    * Main
    */
  def main(args : Array[String]) = {
   
    // Data file
    val fn_fits = args(0).toString
    
    // HDU
    val hdu = args(1).toString
     

    // Column names
    val columns = args(2).toString
    
  
  // isSpherical
  val isSpherical : String = args(3).toBoolean match {
    case true => "spherical"
    case false => "cartesian"
}
    // partitioning
    val grid = args(4).toString
     
    // partitions
    val part = args(5).toInt
     
    // Mode
    val mode = args(6).toString
     
    // Load the data
    ///////////////////////////////spark.sparkContext
    //val  df = spark.read.format("csv").option("header", "true").load("hdfs://134.158.75.222:8020/user/alia/example3D.csv")
    //val  df = spark.read.format("csv").option("header", "true").load("hdfs://134.158.75.222:8020/user/alia/test2d.csv")
     val df = spark.read.format("fits").option("hdu", 1).load(fn_fits)
     
    
    //////////////////////////////
     
   val options = Map(
    "geometry" -> "points",
    "colnames" -> columns,
    "coordSys" -> isSpherical,
    "gridtype" -> grid)


     

  /**
   * Validation
   */
  /**
   * Find the number of levels
   */
  val log2 = (x: Int) => log10(x)/log10(2.0)

  val maximumLevel=(log2(df.count.asInstanceOf[Int]+1))-2 
  //Find the number of partitions
  val maximumPartitions=pow(maximumLevel,2)
  
  if(log2(part)%1==0)
     if(part<=maximumPartitions){
         val df_colid = df.prePartition(options, part)
         val number1=df_colid.repartitionByCol("partition_id", true, part).mapPartitions(part => Iterator(part.size)).collect().toList
         println("number1: "+ number1)
      }
      else 
         println("Please, Maximum number of partitions is "+ maximumPartitions)
   else 
    println("Please determine the number of partitions as 1, 2, 4, 8 and so on")
        

 
  //  val df_colid = df.prePartition(options, part)

   // df_colid.show(100,false)
   // df_colid.groupBy("partition_id").count().show(50,false)   
    // df_colid.printSchema()
     //df_colid.filter($"partition_id">4).show()
     //println(df_colid.filter($"DEC"< -0.001).count())
     //MC it to minimize flukes
   
  //    val number= df_colid.repartitionByCol("partition_id", true, part)
          

  // val number1=df_colid.repartitionByCol("partition_id", true, part).mapPartitions(part => Iterator(part.size)).collect().toList
  //  println("number1: "+ number1)
   
     

  }
}