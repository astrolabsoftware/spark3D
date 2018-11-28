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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

import com.astrolabsoftware.spark3d.geometryObjects.Point3D
import com.astrolabsoftware.spark3d.geometryObjects.ShellEnvelope
import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope
import com.astrolabsoftware.spark3d.spatialOperator.KNN.KNNStandard
import com.astrolabsoftware.spark3d.spatialOperator.WindowQuery.windowQueryFromRDD


/**
  * Main object containing methods to query 3D data sets.
  */
object Queries {

  /**
    * Check if the coordinate system is spherical
    *
    * @param coordSys : Name of the coordinate system (spherical or cartesian)
    * @return true is spherical, false if cartesian. Otherwise raise an error.
    */
  def checkIsSpherical(coordSys: String) : Boolean = {
    coordSys match {
      case "spherical" => true
      case "cartesian" => false
      case _ => throw new AssertionError("""
        Coordinate system not understood! You must choose between:
        spherical, cartesian
        """)
    }
  }

  /**
    * Make a RDD[Point3D] from a DF containing point coordinates.
    * Cast entries to Double.
    *
    * @param df : Input DataFrame
    * @param inputType : Input Spark SQL type as String.
    * @param isSpherical : true if coordinate system is spherical, false if cartesian.
    * @return a RDD[Point3D]
    */
  def createPointRDDFromDF(df: DataFrame, inputType: String, isSpherical: Boolean) : RDD[Point3D] = {
    inputType match {
      case "DoubleType" => df.rdd.map(x => new Point3D(x.getDouble(0), x.getDouble(1), x.getDouble(2), isSpherical))
      case "FloatType" => df.rdd.map(x => new Point3D(x.getFloat(0).toDouble, x.getFloat(1).toDouble, x.getFloat(2).toDouble, isSpherical))
      case "IntegerType" => df.rdd.map(x => new Point3D(x.getInt(0).toDouble, x.getInt(1).toDouble, x.getInt(2).toDouble, isSpherical))
    }
  }

  /**
    * Make a DF with point coordinates from List[(Double, Double, Double)]
    *
    * @param listOfCoordinate : Input list of Tuple of 3 Doubles
    * @param isSpherical : true if coordinate system is spherical, false if cartesian.
    * @return DataFrame with point coordinates
    */
  def createDFFromList(listOfCoordinate: List[(Double, Double, Double)], colnames: Array[String]): DataFrame = {
    // Load Spark implicits
    val locSpark = SparkSession.getActiveSession.get
    import locSpark.implicits._

    // Return a DataFrame
    locSpark.sparkContext.parallelize(listOfCoordinate).toDF(colnames(0), colnames(1), colnames(2))
  }

  /**
    * Make a DF with point coordinates from RDD[Point3D].
    *
    * @param rdd : RDD[Point3D]
    * @param colnames : Array[String] with columnn names.
    * @return DataFrame with point coordinates
    */
  def createDFFromPointRDD(rdd: RDD[Point3D], colnames: Array[String]): DataFrame = {
    // Load Spark implicits
    val locSpark = SparkSession.getActiveSession.get
    import locSpark.implicits._

    // Return a DataFrame
    rdd.map(x => x.getCoordinate).map(x => (x(0), x(1), x(2))).toDF(colnames(0), colnames(1), colnames(2))
  }

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
    val isSpherical : Boolean = checkIsSpherical(coordSys)

    // Assume 3 columns have the same dtype.
    val inputType : String = df.dtypes(0)._2

    val rdd = createPointRDDFromDF(df, inputType, isSpherical)

    // Map target to Point3D
    val targetAsPoint = new Point3D(target(0), target(1), target(2), isSpherical)

    // Perform the KNN search - return a List of Point3D
    val knnList = KNNStandard(rdd, targetAsPoint, k, unique)
      // Get the coordinates of points
      .map(x => x.getCoordinate)
      // map to Tuple
      .map(x => (x(0), x(1), x(2)))

    createDFFromList(knnList, df.columns)
  }

  /**
    * Perform window query, that is match between DF elements and
    * a user-defined window (point, sphere, shell, box).
    *
    * If windowType =
    *   - point: windowCoord = List(x, y, z)
    *   - sphere: windowCoord = List(x, y, z, R)
    *   - shell: windowCoord = List(x, y, z, Rin, Rout)
    *   - box: windowCoord = List(x1, y1, z1, x2, y2, z2, x3, y3, z3)
    * Use (x, y, z) for cartesian or (r, theta, phi) for spherical.
    * Note that box only accepts cartesian coordinates.
    *
    * @param df : Input DataFrame. Must have 3 columns (the coordinate center of objects)
    * @param windowType : point, shell, sphere, or box.
    * @param windowCoord : List of Doubles. The coordinates of the window (see doc above).
    * @param coordSys : String, coordinate system of the points: spherical or cartesian
    * @return DataFrame with the coordinates of the objects inside the window.
    */
  def windowQuery(df: DataFrame, windowType: String, windowCoord: List[Double], coordSys: String) : DataFrame = {
    // Number of coordinates must be 3
    val ok = df.columns.size match {
      case 3 => true
      case _ => throw new AssertionError("""
        Input DataFrame must have 3 columns to perform window query (the coordinate center of objects).
        Use df.select("col1", "col2", "col3")
      """)
    }

    // Definition of the coordinate system. Spherical or cartesian
    val isSpherical : Boolean = checkIsSpherical(coordSys)

    // Assume 3 columns have the same dtype.
    val inputType : String = df.dtypes(0)._2

    val rdd = createPointRDDFromDF(df, inputType, isSpherical)

    // Map target to Point3D
    val windowRDD = windowType match {
      case "point" => {
        val shape = new Point3D(windowCoord(0), windowCoord(1), windowCoord(2), isSpherical)
        windowQueryFromRDD(rdd, shape)
      }
      case "sphere" => {
        val shape = new ShellEnvelope(windowCoord(0), windowCoord(1), windowCoord(2), isSpherical, windowCoord(3))
        windowQueryFromRDD(rdd, shape)
      }
      case "shell" => {
        val shape = new ShellEnvelope(windowCoord(0), windowCoord(1), windowCoord(2), isSpherical, windowCoord(3), windowCoord(4))
        windowQueryFromRDD(rdd, shape)
      }
      case "box" => {
        val ok = isSpherical match {
          case true => throw new AssertionError("""
            Input Box coordinates must have cartesian coordinate!
            """)
          case false => true
        }
        val p1 = new Point3D(windowCoord(0), windowCoord(1), windowCoord(2), false)
        val p2 = new Point3D(windowCoord(3), windowCoord(4), windowCoord(5), false)
        val p3 = new Point3D(windowCoord(6), windowCoord(7), windowCoord(8), false)
        val shape = new BoxEnvelope(p1, p2, p3)
        windowQueryFromRDD(rdd, shape)
      }
      case _ => throw new AssertionError("""
        windowType not understood! You must choose between:
          point, shell, sphere, box

        If windowType =
         - point: windowCoord = List(x, y, z)
         - sphere: windowCoord = List(x, y, z, R)
         - shell: windowCoord = List(x, y, z, Rin, Rout)
         - box: windowCoord = List(x1, y1, z1, x2, y2, z2, x3, y3, z3)
        Use (x, y, z) for cartesian or (r, theta, phi) for spherical.
        Note that box only accepts cartesian coordinates.
      """)
    }

    createDFFromPointRDD(windowRDD, df.columns)
  }
}
