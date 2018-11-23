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
package com.astrolabsoftware.spark3d.python

import scala.collection.JavaConverters._
import java.util.HashMap

import scala.reflect.ClassTag

/**
  * Define explicitly (!) ClassTags so that they can be access from
  * the Python world! See https://github.com/bartdag/py4j/issues/328 for
  * discussion.
  *
  * Important to read:
  * https://www.scala-lang.org/api/2.12.3/scala/reflect/ClassTag.html
  * https://docs.scala-lang.org/overviews/reflection/typetags-manifests.html
  */
object PythonClassTag {

  /**
    * This routine returns the ClassTag of an object.
    * The primary goal is to have a way to pass the ClassTag of a Scala object
    * to the python world.
    *
    * By default in py4j, if you try to call from Python a Scala method/class/etc
    * with Reflection (generic arguments using ClassTag), you will run
    * into trouble unless you manually specify the ClassTag of generic arguments.
    * Let's take an example:
    *
    * // Definition in Scala
    * def method1(in: A) = ...
    * def method2[A : ClassTag](in: A) = ...
    *
    * # From python
    * method1(in) # works
    * method2(in) # fails
    *
    * The reason is that the python constructor for a Scala method with generics
    * needs to know the type of A:
    * method2(in, ClassTagFor_in) # work
    *
    * @param obj : (Any)
    *   Any object for which you want to know the ClassTag
    * @return (ClassTag[_]) the ClassTag of the object
    *
    */
  def classTagFromObject(obj: Any): ClassTag[_] = {
    ClassTag.apply(obj.getClass())
  }

  /**
    * By default, Python dictionaries are converted in java.util.HashMap when
    * used as method arguments by py4j. This is a problem for most Scala methods
    * which need Map as arguments.
    *
    * This method allows the conversion from Python dictionary to scala immutable map in py4j.
    * This should be use in Python as:
    * mydic = {k: v}
    * conv = sc._jvm.com.astrolabsoftware.spark3d.python.PythonClassTag.javaHashMaptoscalaMap
    * scalaMap = conv(mydic)
    * sc._jvm.<...>.myscalaMethod(scalaMap)
    *
    * @param hashMap : java.util.HashMap[String, String]
    * @return Map[String, String]
    */
  def javaHashMaptoscalaMap(hashMap: HashMap[String, String]): Map[String, String] = {
    hashMap.asScala.toMap
  }
}
