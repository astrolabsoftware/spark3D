package com.spark3d.spatialOperator

import com.spark3d.geometryObjects.Shape3D.Shape3D
import com.spark3d.utils.GeometryObjectComparator
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue

object ObjComparator extends Ordering[Shape3D] {

  override def compare(x: Shape3D, y: Shape3D): Int = {

  }

}
object KNNQuery {
    def SpatialKNNQuery[A <: Shape3D, B <:Shape3D](queryObject: A, rdd: RDD[B], k: Int): List[B] = {

      val pq: PriorityQueue[B] = new mutable.PriorityQueue()
          (new GeometryObjectComparator[B](queryObject.center))

      rdd.map(x => {
        if (pq.size < k) {
          pq.enqueue(x)
        } else {

          val currentDist = x.center.distanceTo(queryObject.center)
          // TODO make use of pq.max
          val maxElement = pq.dequeue
          val quDist = maxElement.center.distanceTo(queryObject.center)
          if (currentDist < quDist) {
            pq.enqueue(x)
          } else {
            pq.enqueue(maxElement)
          }
        }
      })
      null
    }
}