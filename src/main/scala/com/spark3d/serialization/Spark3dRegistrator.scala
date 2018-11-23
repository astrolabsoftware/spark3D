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
package com.astrolabsoftware.spark3d.serialization

import org.apache.spark.SparkConf

import com.astrolabsoftware.spark3d.geometryObjects.{Point3D, ShellEnvelope, BoxEnvelope}
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D._
import com.astrolabsoftware.spark3d.spatialPartitioning.{OnionPartitioner, OnionPartitioning, SpatialPartitioner}
import com.astrolabsoftware.spark3d.utils.ExtPointing
import java.util.HashSet

/**
  * Setup a Spark conf with Kryo serializer, and register spark3D classes.
  * TODO: Implement correctly the serialisation of geometryObjects and
  * geometry. See https://github.com/EsotericSoftware/kryo and
  * https://github.com/JulienPeloton/spark3D/issues/28
  *
  */
object Spark3dConf {

  /**
    * Setup a Spark conf with Kryo serializer, and register spark3D classes.
    * In addition, setup the buffer size to 1024 KB, and its max to 1024 MB.
    *
    * @return (SparkConf) Spark configuration with classes registered in Kryo.
    */
  def spark3dConf : SparkConf = {
    // Initialise a new Spark conf
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryoserializer.buffer", "1024k")
    conf.set("spark.kryoserializer.buffer.max", "1024m")

    conf.registerKryoClasses(
      Array(
        classOf[Shape3D],
        classOf[Point3D],
        classOf[ShellEnvelope],
        classOf[BoxEnvelope],
        classOf[SpatialPartitioner],
        classOf[OnionPartitioner],
        classOf[OnionPartitioning],
        classOf[ExtPointing],
        classOf[HashSet[_]]
      )
    )
  }
}
