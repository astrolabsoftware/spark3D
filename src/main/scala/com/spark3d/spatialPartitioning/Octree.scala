package com.spark3d.spatialPartitioning

import com.spark3d.geometry._
import com.spark3d.geometryObjects.Point3D

import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer

/**
  * It is assumed that one can place an object only using its bounding box
  *
  * @param box
  * @param maxItemsPerBox maximum number of items in one box
  */
class Octree(
    val box: BoxEnvelope,
    val maxItemsPerBox: Int = 5)
  extends Serializable {

  // elements inside one box
  private final val elements = new ListBuffer[BoxEnvelope]
  // number of elements points in the box
  private var elementNum = 0
  // the array of children boxes
  private var children: Array[Octree] = _

  val SELF: Int = -1
  val CHILD_U_NW: Int = 0
  val CHILD_U_NE: Int = 1
  val CHILD_U_SW: Int = 2
  val CHILD_U_SE: Int = 3
  val CHILD_L_NW: Int = 4
  val CHILD_L_NE: Int = 5
  val CHILD_L_SW: Int = 6
  val CHILD_L_SE: Int = 7

  def splitBox(): Unit = {
    children = new Array[Octree](8)

    children(CHILD_L_SW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        box.minY, (box.maxY - box.minY) / 2,
        box.minZ, (box.maxZ - box.minZ) / 2),
      maxItemsPerBox)

    children(CHILD_L_SE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        box.minY, (box.maxY - box.minY) / 2,
        box.minZ, (box.maxZ - box.minZ) / 2),
      maxItemsPerBox)

    children(CHILD_L_NW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        (box.maxY - box.minY) / 2, box.maxY,
        box.minZ, (box.maxZ - box.minZ) / 2),
      maxItemsPerBox)

    children(CHILD_L_NE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        (box.maxY - box.minY) / 2, box.maxY,
        box.minZ, (box.maxZ - box.minZ) / 2),
      maxItemsPerBox)

    children(CHILD_U_SW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        box.minY, (box.maxY - box.minY) / 2,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      maxItemsPerBox)

    children(CHILD_U_SE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        box.minY, (box.maxY - box.minY) / 2,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      maxItemsPerBox)

    children(CHILD_U_NW) = new Octree(
      BoxEnvelope.apply(
        box.minX, (box.maxX - box.minX) / 2,
        (box.maxY - box.minY) / 2, box.maxY,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      maxItemsPerBox)

    children(CHILD_U_NE) = new Octree(
      BoxEnvelope.apply(
        (box.maxX - box.minX) / 2, box.maxX,
        (box.maxY - box.minY) / 2, box.maxY,
        (box.maxZ - box.minZ) / 2, box.maxZ),
      maxItemsPerBox)

  }

  private def findRegion(obj: BoxEnvelope, split: Boolean = true): Int = {
    var region = SELF

    if (elementNum >= maxItemsPerBox) {
      if (children == null && split) {
        splitBox
      }

      if (children != null) {
        breakable {
          for (i <- children.indices) {
            if (children(i).box.contains(obj)) {
              region = i
              break
            }
          }
        }
      }
    }
    region
  }

  def insertElement(element: BoxEnvelope): Unit = {

    if (!box.contains(element)) {
      throw new AssertionError(
        """
                elementect to be placed must be smaller than the the
                space encompassed by this Octree.
                """)
    }

    val region = findRegion(element)

    if (region == SELF) {
      elements += element
      elementNum += 1
    } else {
      children(region).insertElement(element)
    }

    if (elementNum >= maxItemsPerBox) {
      for (element <- elements) {
        insertElement(element)
      }
      elements.clear
    }
  }

  def removeElement(element: BoxEnvelope): Unit = {

    if (!box.contains(element)) {
      throw new AssertionError(
        """
                Element to be removed must be smaller than the the
                space encompassed by this Octree.
                """)
    }

    val region = findRegion(element)

    if (region == SELF) {
      elements -= element
    } else {
      children(region).removeElement(element)
    }
  }

  private def findElements(element: BoxEnvelope): ListBuffer[BoxEnvelope] = {

    if (!box.contains(element)) {
      return null
    }

    if (elementNum >= maxItemsPerBox) {
      if (children != null) {
        breakable {
          for (child <- children) {
            val containedElement = child.findElements(element)
            if (containedElement != null) {
              return containedElement
            }
          }
        }
      }
    }
    elements
  }

  def getElements(element: BoxEnvelope): ListBuffer[BoxEnvelope] = {

    if (!box.contains(element)) {
      throw new AssertionError(
        """
                Element to be searched must be smaller than the the
                space encompassed by this Octree.
                """)
    }

    findElements(element)
  }

  def isLeaf(): Boolean = {
    children == null
  }

  def findBox(x: Double, y: Double, z: Double): BoxEnvelope = {
    findBox(new Point3D(x, y, z, false))
  }

  def findBox(p: Point3D): BoxEnvelope = {

    if (!box.contains(p)){
      return null
    }

    for(child <- children) {
      val containingBox = child.findBox(p)
      if (containingBox != null) {
        return containingBox
      }
    }

    box
  }
}
