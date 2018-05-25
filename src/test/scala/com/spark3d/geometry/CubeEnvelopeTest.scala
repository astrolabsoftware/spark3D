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

package com.spark3d.geometry
import scala.math._
import com.spark3d.geometry._
import com.spark3d.geometryObjects.Point3D
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Test class for the cube Envelope methods.
  */
class CubeEnvelopeTest extends FunSuite with BeforeAndAfterAll {

  var valid_env: CubeEnvelope = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val p1 = new Point3D(-5.6, 3.4, 2.8)
    val p2 = new Point3D(7.1, 5.7, 1.2)
    val p3 = new Point3D(3.6, 6.8, 9.3)

    valid_env = new CubeEnvelope(p1, p2, p3)
  }

  test("Can you initialize a null cube Envelope?") {

    val env = new CubeEnvelope()
    assert(env.minX == 0.0)
    assert(env.maxX == -1.0)
    assert(env.minY == 0.0)
    assert(env.maxY == -1.0)
    assert(env.minZ == 0.0)
    assert(env.maxZ == -1.0)
  }

  test("Can you initialize the cube Envelope with three Point3D?") {

    val p1 = new Point3D(0.0, 1.0, 0.0)
    val p2 = new Point3D(0.1, 1.0, 0.0)
    val p3 = new Point3D(1.0, -1.0, 1.0)
    val env = new CubeEnvelope(p1, p2, p3)

    assert(env.minX == 0.0)
    assert(env.maxX == 1.0)
    assert(env.minY == -1.0)
    assert(env.maxY == 1.0)
    assert(env.minZ == 0.0)
    assert(env.maxZ == 1.0)
  }

  test("Can you initialize the cube Envelope with two Point3D?") {

    val p1 = new Point3D(0.0, 1.0, 0.0)
    val p2 = new Point3D(1.0, -1.0, 1.0)
    val env = new CubeEnvelope(p1, p2)

    assert(env.minX == 0.0)
    assert(env.maxX == 1.0)
    assert(env.minY == -1.0)
    assert(env.maxY == 1.0)
    assert(env.minZ == 0.0)
    assert(env.maxZ == 1.0)
  }

  test("Can you initialize the cube Envelope with one Point3D?") {

    val p1 = new Point3D(0.0, 1.0, 0.0)
    val env = new CubeEnvelope(p1)

    assert(env.minX == 0.0)
    assert(env.maxX == 0.0)
    assert(env.minY == 1.0)
    assert(env.maxY == 1.0)
    assert(env.minZ == 0.0)
    assert(env.maxZ == 0.0)
  }

  test("Can you clone an existing Envelop to create a new cube Envelope?") {

    val env = new CubeEnvelope(valid_env)

    assert(env.minX == -5.6)
    assert(env.maxX == 7.1)
    assert(env.minY == 3.4)
    assert(env.maxY == 6.8)
    assert(env.minZ == 1.2)
    assert(env.maxZ == 9.3)
  }

  test("Can you get the minExtent of the cube Envelope?") {
    assert(valid_env.minExtent() == 3.4)
  }

  test("Can you get the maxExtent of the cube Envelope?") {
    assert(valid_env.maxExtent() == 12.7)
  }

  test("Can you get the area of the cube Envelope?") {
    assert(((math floor valid_env.getArea() * 1000) / 1000) == 12.7 * 3.4 * 8.1)
  }

  test("Can you expand the cube Envelope to by given separate extents along all three axes?") {

    val clone_env = new CubeEnvelope(valid_env)
    clone_env.expandBy(1.2, 2.3, 3.4)

    assert(clone_env.minX == -6.8)
    assert((math rint clone_env.maxX  * 10) / 10 == 8.3)
    assert(clone_env.minY == 1.1)
    assert(clone_env.maxY == 9.1)
    assert(clone_env.minZ == -2.2)
    assert((math rint clone_env.maxZ  * 10) / 10 == 12.7)
  }

  test("Can you expand the cube Envelope to include the given Point3D?") {

    val clone_env = new CubeEnvelope(valid_env)
    clone_env.expandToInclude(new Point3D(11.2, 2.3, 3.4))

    assert(clone_env.minX == -5.6)
    assert(clone_env.maxX == 11.2)
    assert(clone_env.minY == 2.3)
    assert(clone_env.maxY == 6.8)
    assert(clone_env.minZ == 1.2)
    assert(clone_env.maxZ == 9.3)
  }

  test("Can you expand the cube Envelope to include the given cube Envelope?") {

    var clone_env = new CubeEnvelope(valid_env)
    clone_env.expandToInclude(new CubeEnvelope(new Point3D(11.2, 2.3, 3.4)))

    assert(clone_env.minX == -5.6)
    assert(clone_env.maxX == 11.2)
    assert(clone_env.minY == 2.3)
    assert(clone_env.maxY == 6.8)
    assert(clone_env.minZ == 1.2)
    assert(clone_env.maxZ == 9.3)
  }

  test("Can you translate/move the cube Envelope by given lengths in all three axes?") {

    val clone_env = new CubeEnvelope(valid_env)
    clone_env.translate(1.1, 2.2, 3.3)

    assert(clone_env.minX == -4.5)
    assert(clone_env.maxX == 8.2)
    assert(clone_env.minY == 5.6)
    assert(clone_env.maxY == 9.0)
    assert(clone_env.minZ == 4.5)
    assert((math rint clone_env.maxZ  * 10) / 10 == 12.6)
  }

  test("Can you get the center of the cube Envelope?") {
    val cen = valid_env.center
    assert(cen.x == 0.75)
    assert(cen.y == 5.1)
    assert(cen.z == 5.25)
  }

  test("Can you find the intersection of the two cube Envelopes?") {

    val p1 = new Point3D(0.0, 1.0, 0.0)
    val p2 = new Point3D(0.1, 1.0, 0.0)
    val p3 = new Point3D(1.0, -1.0, 1.0)
    val env = new CubeEnvelope(p1, p2, p3)

    val inter = valid_env.intersection(env)
    assert(inter.minX == -5.6)
    assert(inter.maxX == 7.1)
    assert(inter.minY == -1.0)
    assert(inter.maxY == 6.8)
    assert(inter.minZ == 0.0)
    assert(inter.maxZ == 9.3)
  }

  test("Can you test if the two cube Envelopes intersect?") {
    val p1 = new Point3D(0.0, 1.0, 0.0)
    val p2 = new Point3D(0.1, 4.0, 0.0)
    val p3 = new Point3D(1.0, -1.0, 1.3)
    val env = new CubeEnvelope(p1, p2, p3)

    assert(valid_env.intersects(env))
    assert(valid_env.intersects(p1, p2, p3))
  }

  test("Can you test if the point intersects the cube Envelope?") {
    assert(valid_env.intersects(-7.1, 7.1, 12.2) == false)
  }

  test("Can you test if the point lies inside of the cube Envelope?") {
    assert(valid_env.covers(5.5, 5.5, 5.5))
  }

  test("Can you test if the input Envelope lies inside of the cube Envelope?") {
    val p1 = new Point3D(2.2, 3.8, 1.5)
    val p2 = new Point3D(6.0, 5.4, 2.6)
    val p3 = new Point3D(4.2, 6.2, 8.2)
    val env = new CubeEnvelope(p1, p2, p3)

    assert(valid_env.covers(env))
  }

  test("Can you compute the distance between the two cube Envelopes?") {
    val p1 = new Point3D(2.2, 9.1, 1.5)
    val p2 = new Point3D(6.0, 7.1, 2.6)
    val p3 = new Point3D(4.2, 12.2, 8.2)
    val env = new CubeEnvelope(p1, p2, p3)

    assert((math rint valid_env.distance(env) * 10) / 10  == 0.3)
  }

  test("Can you check if the two cube Envelopes are equal?") {
    val p1 = new Point3D(2.2, 9.1, 1.5)
    val p2 = new Point3D(6.0, 7.1, 2.6)
    val p3 = new Point3D(4.2, 12.2, 8.2)
    val env = new CubeEnvelope(p1, p2, p3)

    assert((math rint valid_env.distance(env) * 10) / 10 == 0.3)
  }

  test("Can you get the string representation of the cube Envelope?") {
    val str = "Env[" +
      valid_env.minX + " : " + valid_env.maxX + ", " +
      valid_env.minY + " : " + valid_env.maxY + ", " +
      valid_env.minZ + " : " + valid_env.maxZ + ", " +
      "]"

    assert(valid_env.toString == str)
  }

}