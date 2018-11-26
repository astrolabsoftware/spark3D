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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.functions.udf
// import org.apache.spark.sql.functions.col
// import org.apache.spark.sql.functions.lit
import scala.collection.JavaConverters._

import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.spatialOperator.KNN.KNNStandard


/**
  * Main object containing methods to query 3D data sets.
  */
object Queries {

  /**
    * Finds the K nearest neighbors of the query object.
    * The naive implementation here searches through all the the objects in
    * the DataFrame to get the KNN. The nearness of the objects here
    * is decided on the basis of the distance between their centers.
    *
    * @param df : Input DataFrame. Must have 3 columns (the coordinate center of objects)
    * @param target : List of 3 Double. The coordinates of the targeted point.
    * @param k : Integer, number of neighbours to find.
    * @param coordSys : String, coordinate system of the points: spherical or cartesian
    * @param unique : Boolean. If true, returns only distinct objects. Default is false.
    * @return DataFrame with the coordinates of the k neighbours found.
    */
  def KNN(df: DataFrame, target: List[Double], k : Int, coordSys: String, unique: Boolean = false) : DataFrame = {
    // Number of coordinates must be 3
    val ok = df.columns.size match {
      case 3 => true
      case _ => throw new AssertionError("""
        Input DataFrame must have 3 columns to perform KNN (the coordinate center of objects).
        Use df.select("col1", "col2", "col3")
      """)
    }

    // Definition of the coordinate system. Spherical or cartesian
    val isSpherical : Boolean = coordSys match {
      case "spherical" => true
      case "cartesian" => false
      case _ => throw new AssertionError("""
        Coordinate system not understood! You must choose between:
        spherical, cartesian
        """)
    }

    // Assume 3 columns have the same dtype.
    val inputType = df.dtypes(0)._2

    // Map to coordinates to Point3D. Must be Double.
    val rdd = inputType match {
      case "DoubleType" => df.rdd.map(x => new Point3D(x.getDouble(0), x.getDouble(1), x.getDouble(2), isSpherical))
      case "FloatType" => df.rdd.map(x => new Point3D(x.getFloat(0).toDouble, x.getFloat(1).toDouble, x.getFloat(2).toDouble, isSpherical))
      case "IntegerType" => df.rdd.map(x => new Point3D(x.getInt(0).toDouble, x.getInt(1).toDouble, x.getInt(2).toDouble, isSpherical))
    }

    // Map target to Point3D
    val targetAsPoint = new Point3D(target(0), target(1), target(2), isSpherical)

    // Perform the KNN search - return an array of Point3D
    val out = KNNStandard(rdd, targetAsPoint, k, unique)
      // Get the coordinates of points
      .map(x => x.getCoordinate)
      // map to Tuple
      .map(x => (x(0), x(1), x(2)))

    // Load Spark implicits
    val locSpark = SparkSession.getActiveSession.get
    import locSpark.implicits._

    // Return a DataFrame
    locSpark.sparkContext.parallelize(out).toDF(df.columns(0), df.columns(1), df.columns(2))
  }
}
