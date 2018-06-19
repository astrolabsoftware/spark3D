/*
 * Copyright 2018 Mayur Bhosale
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

package com.spark3d.geometryObjects

import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Test class for the cube Envelope methods.
  */
class ShellEnvelopeTest extends FunSuite with BeforeAndAfterAll {

  var valid_env: ShellEnvelope = _
  var null_env: ShellEnvelope = _

  val isSpherical = false


  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val p2 = new Point3D(0.0, 0.0, 0.0, isSpherical)

    valid_env = new ShellEnvelope(p2, 5.0, 10.0)
    null_env = new ShellEnvelope(p2, 10.0, 5.0)
  }

  test("Can you initialize a null sphere Envelope?") {

    val env = new ShellEnvelope()
    assert(env.outerRadius == -2.0)
    assert(env.innerRadius == -1.0)
  }

  test("Can you initialize a sphere Envelope defined by center, inner and outer radius?") {

    val env_v1 = new ShellEnvelope(0.0, 0.0, 0.0, isSpherical, 2.0, 2.0)
    assert(!env_v1.isNull)
    val p2 = new Point3D(0.0, 0.0, 0.0, isSpherical)
    val env = new ShellEnvelope(p2, 2.0, 3.0)
    assert(!env.isNull)
  }

  test("Can you initialize a sphere Envelope defined by a center, and single radius (a sphere)?") {

    val p = new Point3D(0.0, 0.0, 0.0, isSpherical)
    val env = new ShellEnvelope(p, 3.0)
    assert(!env.isNull)
  }

  test("Can you initialize a sphere Envelope defined by center coordinates, and single radius (a sphere)?") {

    val env = new ShellEnvelope(0.0, 0.0, 0.0, isSpherical, 3.0)
    assert(!env.isNull)
  }

  test ("Can you set a sphere Envelope to null?") {
    val env = new ShellEnvelope(valid_env)
    env.setToNull
    assert(env.isNull)
  }

  test ("Can you get the bounding box around the Shell?") {
    val pSph = new Point3D(0.0, 0.0, 0.0, false)
    val shell = new ShellEnvelope(pSph, 1.0)
    val bb = BoxEnvelope(-1.0, 1.0, -1.0, 1.0, -1.0, 1.0)
    assert(shell.getEnvelope.isEqual(bb))

    val pCart = new Point3D(0.0, 0.0, 0.0, true)
    val shell2 = new ShellEnvelope(pCart, 1.0)
    val bb2 = BoxEnvelope(-1.0, 1.0, -1.0, 1.0, -1.0, 1.0)
    assert(shell2.getEnvelope.isEqual(bb2))
  }

  test("Can you get the area of the Shell Envelope?") {
    val area_null = null_env.getArea
    assert(area_null == 0.0)
    val area = valid_env.getArea
    assert((math floor area * 1000) / 1000 == 942.477)
  }

  test("Can you expand the shell Envelope so that it includes the input Point?") {
    var env = new ShellEnvelope(valid_env)

    val p1: Point3D = new Point3D(0.0, 0.0, 7.0, isSpherical)
    val p2: Point3D = new Point3D(0.0, 4.0, 0.0, isSpherical)
    val p3: Point3D = new Point3D(0.0, 0.0, 12.0, isSpherical)

    null_env.expandToInclude(p2)
    assert(null_env.isNull)

    // case when point already lies inside the shell Envelope
    env.expandToInclude(p1)
    assert(!env.isNull)
    assert(env.innerRadius == 5.0)
    assert(env.outerRadius == 10.0)

    // case for expansion of the inner radius
    env.expandToInclude(p2)
    assert(!env.isNull)
    assert(env.innerRadius == 4.0)
    assert(env.outerRadius == 10.0)

    env = new ShellEnvelope(valid_env)
    // case for expansion of the outer radius
    env.expandToInclude(p3)
    assert(!env.isNull)
    assert(env.innerRadius == 5.0)
    assert(env.outerRadius == (12.0 + 0.1))
  }

  test("Can you expand the shell Envelope so that it includes the input Shell Envelope?") {
    var env = new ShellEnvelope(valid_env)
    val new_env = new ShellEnvelope(valid_env)

    null_env.expandToInclude(env)
    assert(null_env.isNull)

    // case when the input shell Envelope already lies inside this shell Envelope
    new_env.innerRadius = 6.0
    new_env.outerRadius = 7.0
    env.expandToInclude(new_env)
    assert(!env.isNull)
    assert(env.innerRadius == 5.0)
    assert(env.outerRadius == 10.0)

    env = new ShellEnvelope(valid_env)
    // case for expansion of the inner radius
    new_env.innerRadius = 4.0
    new_env.outerRadius = 9.0
    env.expandToInclude(new_env)
    assert(!env.isNull)
    assert(env.innerRadius== 4.0)
    assert(env.outerRadius == 10.0)

    env = new ShellEnvelope(valid_env)
    // case for expansion of the outer radius
    new_env.innerRadius = 7.0
    new_env.outerRadius = 12.0
    env.expandToInclude(new_env)
    assert(!env.isNull)
    assert(env.innerRadius == 5.0)
    assert(env.outerRadius == (12.0 + 0.1))

    env = new ShellEnvelope(valid_env)
    // case for expansion of both radii
    new_env.innerRadius = 4.0
    new_env.outerRadius = 12.0
    env.expandToInclude(new_env)
    assert(!env.isNull)
    assert(env.innerRadius == 4.0)
    assert(env.outerRadius == (12.0 + 0.1))
  }

  test("Can you expand both inner and outer radius fo the Shell Envelope?") {
    null_env.expandBy(2.6)
    assert(null_env.isNull)

    val env = new ShellEnvelope(valid_env)
    env.expandBy(2.6)
    assert(!env.isNull)
    assert(env.innerRadius == 7.6)
    assert(env.outerRadius == 12.6)
  }

  test("Can you expand the inner radius of the shell Envelope and ensure that the shell Envelope is still valid?") {
    val env = new ShellEnvelope(valid_env)

    null_env.expandInnerRadius(2.6)
    assert(null_env.isNull)

    env.expandInnerRadius(2.6)
    assert(!env.isNull)
    assert(env.innerRadius == 7.6)

    env.expandInnerRadius(13.4)
    assert(env.isNull)
  }

  test("Can you expand the outer radius of the shell Envelope?") {
    val env = new ShellEnvelope(valid_env)

    null_env.expandInnerRadius(2.6)
    assert(null_env.isNull)

    env.expandOuterRadius(2.6)
    assert(!env.isNull)
    assert(env.outerRadius == 12.6)
  }

  test("Can you check if the two shell Envelopes intersect each other?") {
    val env = new ShellEnvelope(valid_env)
    assert(!null_env.intersects(env))

    env.innerRadius = 2.0
    env.outerRadius = 4.0
    assert(!valid_env.intersects(env))

    val p2 = new Point3D(4.0, 6.0, 1.0, isSpherical)
    val env2 = new ShellEnvelope(p2, 5.0, 10.0)
    assert(env2.intersects(valid_env))

    val p3 = new Point3D(30.0, 40.0, 50.0, isSpherical)
    val env3 = new ShellEnvelope(p3, 5.0, 10.0)
    assert(!env3.intersects(valid_env))

  }

  test("Can you check if a shell and a point intersect each other?") {
    val env = new ShellEnvelope(valid_env)
    assert(!null_env.intersects(env))

    env.innerRadius = 0.0
    env.outerRadius = 4.0

    val p = new Point3D(1.0, 1.0, 1.0, isSpherical)
    assert(env.intersects(p))

    val p2 = new Point3D(10.0, 10.0, 10.0, isSpherical)
    assert(!env.intersects(p2))

  }

  test("Can you check if a shell and a box Envelope intersect each other?") {

    val p1 = new Point3D(0.0, 1.0, 0.0, isSpherical)
    val p2 = new Point3D(0.1, 4.0, 0.0, isSpherical)
    val p3 = new Point3D(1.0, -1.0, 1.3, isSpherical)
    val envCube = new BoxEnvelope(p1, p2, p3)
    val envSphere = new ShellEnvelope(p1, 1.0)

    assert(envSphere.intersects(envCube))
  }

  test("Can you check if the input shell Envelope is completely contained by the another shell Envelope?") {
    val env = new ShellEnvelope(valid_env)

    assert(!null_env.contains(env))

    env.innerRadius = 2.0
    env.outerRadius = 4.0
    assert(valid_env.contains(env))

    val p2 = new Point3D(4.0, 6.0, 1.0, isSpherical)
    val env2 = new ShellEnvelope(p2, 5.0, 10.0)
    env.innerRadius = 6.0
    env.outerRadius = 8.0
    assert(!valid_env.contains(env2))
  }

  test("Can you check if the input Point3D lies in the shell Envelope?") {
    val p: Point3D = new Point3D(4.0, 4.0, 4.0, isSpherical)
    assert(!null_env.isPointInShell(p))
    assert(valid_env.isPointInShell(p))

    val p2: Point3D = new Point3D(2.0, 7.0, 8.0, isSpherical)
    assert(!valid_env.isPointInShell(p2))

  }

  test("Can you check if the two shell Envelopes are equal or not?") {
    val env = new ShellEnvelope(valid_env)
    assert(!null_env.isEqual(valid_env))
    assert(env.isEqual(valid_env))

    env.innerRadius = 2.2
    assert(!env.isEqual(valid_env))
  }

  test("Can you check if the input Point3D lies in the shell Envelope using the ShellEnvelope Object?") {
    val p0: Point3D = new Point3D(4.0, 4.0, 4.0, isSpherical)
    val center: Point3D = new Point3D(0.0, 0.0, 0.0, isSpherical)

    // case for innerRadius > outerRadius, i.e. invalid shell
    assert(!ShellEnvelope.isPointInShell(10.0, 5.0, center, p0))

    assert(ShellEnvelope.isPointInShell(5.0, 10.0, center, p0))

    val p1: Point3D = new Point3D(2.0, 7.0, 8.0, isSpherical)
    assert(!ShellEnvelope.isPointInShell(5.0, 10.0, center, p1))
  }
}
