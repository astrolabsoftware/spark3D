package com.spark3d.utils

import com.spark3d.geometryObjects.Point3D
import com.spark3d.geometryObjects.Shape3D.Shape3D

class GeometryObjectComparator[A <: Shape3D]
    (val queryObjectCenter: Point3D) extends Ordering[A] {

  override def compare(x: A, y: A): Int = {
    val dist1 = x.center.distanceTo(queryObjectCenter)
    val dist2 = y.center.distanceTo(queryObjectCenter)

    dist1 compare dist2
  }

}
