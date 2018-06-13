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
class BoxEnvelopeTest extends FunSuite with BeforeAndAfterAll {

  var valid_env: BoxEnvelope = _
  var null_env: BoxEnvelope = _

  val isSpherical = false

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val p1 = new Point3D(-5.6, 3.4, 2.8, isSpherical)
    val p2 = new Point3D(7.1, 5.7, 1.2, isSpherical)
    val p3 = new Point3D(3.6, 6.8, 9.3, isSpherical)

    valid_env = new BoxEnvelope(p1, p2, p3)
    null_env = new BoxEnvelope(p1, p2, p3)
  }

  test("Can you initialize a null cube Envelope?") {

    val env = new BoxEnvelope()
    assert(env.minX == 0.0)
    assert(env.maxX == -1.0)
    assert(env.minY == 0.0)
    assert(env.maxY == -1.0)
    assert(env.minZ == 0.0)
    assert(env.maxZ == -1.0)
  }

  test("Can you initialize the cube Envelope with three Point3D?") {

    val p1 = new Point3D(0.0, 1.0, 0.0, isSpherical)
    val p2 = new Point3D(0.1, 1.0, 0.0, isSpherical)
    val p3 = new Point3D(1.0, -1.0, 1.0, isSpherical)
    val env = new BoxEnvelope(p1, p2, p3)

    assert(env.minX == 0.0)
    assert(env.maxX == 1.0)
    assert(env.minY == -1.0)
    assert(env.maxY == 1.0)
    assert(env.minZ == 0.0)
    assert(env.maxZ == 1.0)
  }

  test("Can you initialize the cube Envelope with two Point3D?") {

    val p1 = new Point3D(0.0, 1.0, 0.0, isSpherical)
    val p2 = new Point3D(1.0, -1.0, 1.0, isSpherical)
    val env = new BoxEnvelope(p1, p2)

    assert(env.minX == 0.0)
    assert(env.maxX == 1.0)
    assert(env.minY == -1.0)
    assert(env.maxY == 1.0)
    assert(env.minZ == 0.0)
    assert(env.maxZ == 1.0)
  }

  test("Can you initialize the cube Envelope with one Point3D?") {

    val p1 = new Point3D(0.0, 1.0, 0.0, isSpherical)
    val env = new BoxEnvelope(p1)

    assert(env.minX == 0.0)
    assert(env.maxX == 0.0)
    assert(env.minY == 1.0)
    assert(env.maxY == 1.0)
    assert(env.minZ == 0.0)
    assert(env.maxZ == 0.0)
  }

  test("Can you clone an existing Envelop to create a new cube Envelope?") {

    val env = new BoxEnvelope(valid_env)

    assert(env.minX == -5.6)
    assert(env.maxX == 7.1)
    assert(env.minY == 3.4)
    assert(env.maxY == 6.8)
    assert(env.minZ == 1.2)
    assert(env.maxZ == 9.3)
  }

  test("Can you set an cube Envelope to null?") {
    null_env.setToNull
    assert(null_env.isNull)
  }

  test("Can you assert that the x/y/zLength of the Envelope is 0.0?") {
    assert(null_env.getXLength == 0.0)
    assert(null_env.getYLength == 0.0)
    assert(null_env.getZLength == 0.0)
  }

  test("Can you get the minExtent of the cube Envelope?") {
    assert(null_env.minExtent == 0.0)
    assert(valid_env.minExtent == 3.4)
  }

  test("Can you get the maxExtent of the cube Envelope?") {
    assert(null_env.maxExtent == 0.0)
    assert(valid_env.maxExtent == 12.7)
  }

  test("Can you get the volume of the cube Envelope?") {
    assert(((math floor valid_env.getVolume * 1000) / 1000) == 12.7 * 3.4 * 8.1)
  }

  test("Can you expand the cube Envelope to by given same extents along all three axes?") {

    val clone_env = new BoxEnvelope(valid_env)
    clone_env.expandBy(2.3)

    assert((math floor clone_env.maxX * 100) / 100 == 9.39)
    assert(clone_env.minY == 1.1)
    assert(clone_env.maxY == 9.1)
    assert((math floor clone_env.minZ * 100) / 100 == -1.1)
    assert((math floor clone_env.maxZ * 100) / 100 == 11.6)
  }

  test("Can you expand the cube Envelope to by given separate extents along all three axes?") {

    null_env.expandBy(1.2, 2.3, 3.4)
    assert(null_env.isNull)

    val clone_env = new BoxEnvelope(valid_env)
    clone_env.expandBy(1.2, 2.3, 3.4)


    assert(clone_env.minX == -6.8)
    assert((math rint clone_env.maxX  * 10) / 10 == 8.3)
    assert(clone_env.minY == 1.1)
    assert(clone_env.maxY == 9.1)
    assert(clone_env.minZ == -2.2)
    assert((math rint clone_env.maxZ  * 10) / 10 == 12.7)
  }

  test("Can you expand the cube Envelope to include the given Point3D?") {

    val clone_null_env = new BoxEnvelope(null_env)
    clone_null_env.expandToInclude(new Point3D(11.2, 2.3, 3.4, isSpherical))
    assert(clone_null_env.minX == 11.2)
    assert(clone_null_env.maxX == 11.2)
    assert(clone_null_env.minY == 2.3)
    assert(clone_null_env.maxY == 2.3)
    assert(clone_null_env.minZ == 3.4)
    assert(clone_null_env.maxZ == 3.4)

    val clone_env = new BoxEnvelope(valid_env)
    clone_env.expandToInclude(new Point3D(11.2, 2.3, -2.3, isSpherical))

    assert(clone_env.minX == -5.6)
    assert(clone_env.maxX == 11.2)
    assert(clone_env.minY == 2.3)
    assert(clone_env.maxY == 6.8)
    assert(clone_env.minZ == -2.3)
    assert(clone_env.maxZ == 9.3)

    // test cases for expansion of the min X and max Y,Z coordinates
    val clone_env_rev = new BoxEnvelope(valid_env)
    clone_env_rev.expandToInclude(new Point3D(-7.8, 12.3, 18.5, isSpherical))

    assert(clone_env_rev.minX == -7.8)
    assert(clone_env_rev.maxX == 7.1)
    assert(clone_env_rev.minY == 3.4)
    assert(clone_env_rev.maxY == 12.3)
    assert(clone_env_rev.minZ == 1.2)
    assert(clone_env_rev.maxZ == 18.5)
  }

  test("Can you expand the cube Envelope to include the given cube Envelope?") {


    var clone_env = new BoxEnvelope(valid_env)
    clone_env.expandToInclude(null_env)
    assert(clone_env.isEqual(valid_env))

    val clone_null_env = new BoxEnvelope(null_env)
    clone_null_env.expandToInclude(valid_env)
    assert(clone_null_env.isEqual(valid_env))

    clone_env.expandToInclude(new BoxEnvelope(new Point3D(11.2, 2.3, -3.4, isSpherical)))

    assert(clone_env.minX == -5.6)
    assert(clone_env.maxX == 11.2)
    assert(clone_env.minY == 2.3)
    assert(clone_env.maxY == 6.8)
    assert(clone_env.minZ == -3.4)
    assert(clone_env.maxZ == 9.3)

    // test cases for expansion of the min X and max Y,Z coordinates
    var clone_env_rev = new BoxEnvelope(valid_env)
    clone_env_rev.expandToInclude(new BoxEnvelope(new Point3D(-7.8, 12.3, 18.5, isSpherical)))

    assert(clone_env_rev.minX == -7.8)
    assert(clone_env_rev.maxX == 7.1)
    assert(clone_env_rev.minY == 3.4)
    assert(clone_env_rev.maxY == 12.3)
    assert(clone_env_rev.minZ == 1.2)
    assert(clone_env_rev.maxZ == 18.5)

  }

  test("Can you translate/move the cube Envelope by given lengths in all three axes?") {

    null_env.translate(1.1, 2.2, 3.3)
    assert(null_env.isNull)

    val clone_env = new BoxEnvelope(valid_env)
    clone_env.translate(1.1, 2.2, 3.3)

    assert(clone_env.minX == -4.5)
    assert(clone_env.maxX == 8.2)
    assert(clone_env.minY == 5.6)
    assert(clone_env.maxY == 9.0)
    assert(clone_env.minZ == 4.5)
    assert((math rint clone_env.maxZ  * 10) / 10 == 12.6)
  }

  test("Can you get the center of the cube Envelope?") {
    // A null Box has a non-null center by construction
    // as you need valid points to construct a Box, and only afterwards
    // you will null it (but center is a val, boundaries are var).
    val cen = valid_env.center
    val cen_null = null_env.center
    assert(cen.x == 0.75 && cen_null.x == 0.75)
    assert(cen.y == 5.1 && cen_null.y == 5.1)
    assert(cen.z == 5.25 && cen_null.z == 5.25)
  }

  test("Can you find the intersection of the two cube Envelopes?") {

    assert(valid_env.intersection(null_env) == null)

    val p1 = new Point3D(0.0, 1.0, 0.0, isSpherical)
    val p2 = new Point3D(0.1, 1.0, 0.0, isSpherical)
    val p3 = new Point3D(-11.0, -1.0, 1.0, isSpherical)
    val env = new BoxEnvelope(p1, p2, p3)

    val inter = valid_env.intersection(env)
    assert(inter.minX == -11.0)
    assert(inter.maxX == 7.1)
    assert(inter.minY == -1.0)
    assert(inter.maxY == 6.8)
    assert(inter.minZ == 0.0)
    assert(inter.maxZ == 9.3)

    val env_rev = new BoxEnvelope(new Point3D(12.2, 12.2, 12.2, isSpherical))

    val inter_rev = valid_env.intersection(env_rev)
    assert(inter_rev.minX == -5.6)
    assert(inter_rev.maxX == 12.2)
    assert(inter_rev.minY == 3.4)
    assert(inter_rev.maxY == 12.2)
    assert(inter_rev.minZ == 1.2)
    assert(inter_rev.maxZ == 12.2)
  }

  test("Can you test if the two cube Envelopes intersect?") {

    assert(!valid_env.intersects(null_env))

    val p1 = new Point3D(0.0, 1.0, 0.0, isSpherical)
    val p2 = new Point3D(0.1, 4.0, 0.0, isSpherical)
    val p3 = new Point3D(1.0, -1.0, 1.3, isSpherical)
    val env = new BoxEnvelope(p1, p2, p3)

    assert(valid_env.intersects(env))
    assert(valid_env.intersects(p1, p2, p3))
  }

  test("Can you test if the point intersects the cube Envelope?") {
    assert(null_env.intersects(-3.1, 4.2, 12.2) == false)
    assert(valid_env.intersects(-3.1, 4.2, 12.2) == false)
  }

  test("Can you test if the area defined by the three Point 3D intersects the cube Envelope?") {


    //test for intersection in x-plane
    val p0: Point3D = new Point3D(12.2, 4.2, 12.2, isSpherical)
    assert(!valid_env.intersects(p0, p0, p0))
    val p1: Point3D = new Point3D(-12.2, 4.2, 12.2, isSpherical)
    assert(!valid_env.intersects(p1, p1, p1))

    //test for intersection in y-plane
    val p2: Point3D = new Point3D(5.4, -12.2, 12.2, isSpherical)
    assert(!valid_env.intersects(p2, p2, p2))
    val p3: Point3D = new Point3D(5.4, 12.2, 12.2, isSpherical)
    assert(!valid_env.intersects(p3, p3, p3))

    //test for intersection in z-plane
    val p4: Point3D = new Point3D(5.4, 5.4, -12.2, isSpherical)
    assert(!valid_env.intersects(p4, p4, p4))
    val p5: Point3D = new Point3D(5.4, 5.4, 12.2, isSpherical)
    assert(!valid_env.intersects(p5, p5, p5))

    assert(!null_env.intersects(p5, p5, p5))
  }

  test("Can you test if the point lies inside of the cube Envelope?") {
    assert(!null_env.contains(5.5, 5.5, 5.5))
    assert(valid_env.contains(5.5, 5.5, 5.5))
  }

  test("Can you test if the Point3D lies inside of the cube Envelope?") {
    assert(valid_env.contains(new Point3D(5.5, 5.5, 5.5, isSpherical)))
  }

  test("Can you test if the input Envelope lies inside of the cube Envelope?") {
    val p1 = new Point3D(2.2, 3.8, 1.5, isSpherical)
    val p2 = new Point3D(6.0, 5.4, 2.6, isSpherical)
    val p3 = new Point3D(4.2, 6.2, 8.2, isSpherical)
    val env = new BoxEnvelope(p1, p2, p3)

    assert(valid_env.contains(env))

    assert(!null_env.contains(env))
  }

  test("Can you compute the distance between the two cube Envelopes?") {
    val p1 = new Point3D(12.2, 9.1, 1.5, isSpherical)
    val p2 = new Point3D(6.0, 7.1, 2.6, isSpherical)
    val p3 = new Point3D(4.2, 12.2, 12.2, isSpherical)
    val env = new BoxEnvelope(p1, p2, p3)

    assert((math rint valid_env.distance(env) * 10) / 10  == 0.3)

    //test for case when the point the point is contained by the cube Envelope
    val p4 = new Point3D(0.0, 1.0, 0.0, isSpherical)
    val p5 = new Point3D(0.1, 4.0, 0.0, isSpherical)
    val p6 = new Point3D(1.0, -1.0, 1.3, isSpherical)
    val env_rev = new BoxEnvelope(p4, p5, p6)

    assert(valid_env.distance(env_rev) == 0.0)

    val p7 = new Point3D(12.0, 12.0, 12.0, isSpherical)
    val env_rev_1 = new BoxEnvelope(p7)

    var dis = valid_env.distance(env_rev_1)
    assert((math rint dis * 1000) / 1000 == 7.638)

    val p8 = new Point3D(-12.0, -12.0, -12.0, isSpherical)
    val env_rev_2 = new BoxEnvelope(p8)

    dis = valid_env.distance(env_rev_2)
    assert((math rint dis * 1000) / 1000 == 21.269)
  }

  test("Can you check if the two cube Envelopes are equal?") {
    val p1 = new Point3D(2.2, 9.1, 1.5, isSpherical)
    val p2 = new Point3D(6.0, 7.1, 2.6, isSpherical)
    val p3 = new Point3D(4.2, 12.2, 8.2, isSpherical)
    val env = new BoxEnvelope(p1, p2, p3)

    assert(!env.isEqual(valid_env))
    assert(!env.isEqual(null))
    assert(!null_env.isEqual(env))
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
