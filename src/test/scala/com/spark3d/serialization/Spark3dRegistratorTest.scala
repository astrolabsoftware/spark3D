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
package com.spark3d.serialization

import com.spark3d.serialization.Spark3dConf.spark3dConf

import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Test class for spark3D serialisation
  */
class Spark3dRegistratorTest extends FunSuite with BeforeAndAfterAll {

  test("Did you register all classes in Kryo?") {
    val conf = spark3dConf

    val numberOfClassesRegistered = conf.getAll
      .filter(x=> x._1 == "spark.kryo.classesToRegister")
      .flatMap(x => x._2.split(","))
      .size

    // Update this test as soon as you register new classes
    assert(numberOfClassesRegistered == 10)
  }

  test("Do you require registration via Kryo?") {
    val conf = spark3dConf

    val requiredRegistration = conf.getAll
      .filter(x=> x._1 == "spark.kryo.registrationRequired")
      .map(x => x._2.toBoolean)
      .toList(0)

    assert(requiredRegistration)
  }
}
