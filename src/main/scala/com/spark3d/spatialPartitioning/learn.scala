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

import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.Column
import org.apache.spark.sql._



import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue

/**
  * Octree is a 3D extension of Quadtree where in at each stage node (Cuboid)
  * (instead of rectangle in Quadtree case) is split into 8 sub nodes.
  *  Each node can contain
  * It is assumed that one can place an object only using its bounding box, a BoxEnvelope
  *
  * @param box root node of this octree
  * @param maxItemsPerNode maximum number of items per Cuboid (box)
  */

/**
  * Octree is a 3D extension of Quadtree where in at each stage node (Cuboid)
  * (instead of rectangle in Quadtree case) is split into 8 sub nodes.
  *
  * @param box BoxEnvelope (boundary) of the tree rooted at this node
  * @param level The depth of the node compared to the root of original tree
  * @param maxItemsPerNode maximum number of elements per node
  * @param maxLevel maximum level upto which tree can grow
  */
class learn(df:DataFrame){

   
  // df.printSchema()
  // df.show(50,false)
 //var df1=df.write.partitionBy("RA")
// val df1= df.repartition(4).withColumn("partition_id", spark_partition_id())
// df1.show(50,false)

def add1(s: String): String = {
 
  return "test"
}

 //val udf1 = udf[Int, String]{s: String => 5 }
   val udf1 = udf[String,String](add1)
  //val udf1=udf{s: String => s+"1"}
val x=df.withColumn("id", udf1(col("RA"))       )
 x.printSchema
 x.show
 
//val placePointsUDF = udf[Int, Double, Double, Double, Boolean](partitioner.placePoints)



  
} 
