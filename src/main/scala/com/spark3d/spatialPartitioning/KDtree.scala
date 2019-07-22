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
package com.astrolabsoftware.spark3d.spatialPartitioning

import com.astrolabsoftware.spark3d.geometryObjects._
import com.astrolabsoftware.spark3d.geometryObjects.Shape3D.Shape3D
import com.astrolabsoftware.spark3d.geometryObjects.Point3D

import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue

/**Description: This class is responsible for:
 * building Balanced KDtree for 3D points
 * Printing the B2DT
 * */
 
class KDtree() extends Serializable {
  var point:Point3D=null
  var box: BoxEnvelope=null
  var level: Int=0
  var parent: KDtree = null
  var left: KDtree = null
  var right: KDtree = null
  var check:Int=0
     
  /**
   * Insert Element to build KDtree
   */
  def insertElement(current:KDtree, value:Point3D, level:Int):KDtree={
     
     var currentLevel=level%3
      if(current==null)
        return null
     // For x dimension
     if(currentLevel==0){
       //left side
       if(value.x<current.point.x){
         if(current.left!=null ){
           insertElement(current.left,value,level+1)
         }
         else {
            current.left=new KDtree()
            current.left.point=value
            current.left.parent=current
            current.left.level=level+1
            current.left.box=BoxEnvelope.apply(box.minX,point.x,box.minY,box.maxY,box.minZ,box.maxZ)
         }
       }
       //right side
       else {
           if(current.right!=null ){
              insertElement(current.right,value,level+1)
            }
            else {
                 current.right=new KDtree()
                 current.right.point=value
                 current.right.parent=current
                 current.right.level=level+1
                 current.right.box=BoxEnvelope.apply(point.x,box.maxX,box.minY,box.maxY,box.minZ,box.maxZ)

                 }
       }   
     }// end of x dim

     
     // For y dimension
      else  if(currentLevel==1){
       //left side
           if(value.y<current.point.y){
               if(current.left!=null ){
                  insertElement(current.left,value,level+1)
                }
               else {
                   current.left=new KDtree()
                   current.left.point=value
                   current.left.parent=current
                   current.left.level=level+1
                   current.left.box=BoxEnvelope.apply(box.minX,box.maxX,box.minY,point.y,box.minZ,box.maxZ)
         }
       }
       //right side
          else {
             if(current.right!=null ){
                insertElement(current.right,value,level+1)
            }
            else {
                 current.right=new KDtree()
                 current.right.point=value
                 current.right.parent=current
                 current.right.level=level+1
                 current.right.box=BoxEnvelope.apply(box.minX,box.maxX,point.y,box.maxX,box.minZ,box.maxZ)
                 
                  }
       }   
     }// end of y dim

      // For z dimension
      else {
        //left side
            if(value.z<current.point.z){
                if(current.left!=null ){
                   insertElement(current.left,value,level+1)
                 }
                  else {
                     current.left=new KDtree()
                     current.left.point=value
                     current.left.parent=current
                     current.left.level=level+1
                     current.left.box=BoxEnvelope.apply(box.minX,box.maxX,box.minY,box.maxX,box.minZ,point.z)
          }
        }
           //right side
           else {
               if(current.right!=null ){
                  insertElement(current.right,value,level+1)
             }
             else {
                  current.right=new KDtree()
                  current.right.point=value
                  current.right.parent=current
                  current.right.level=level+1
                  current.right.box=BoxEnvelope.apply(box.minX,box.maxX,box.minY,box.maxX,point.z,box.maxZ)
                  }
        }   
      }// end of z dim
      current
  } //end of inserElement mehod
  
  /**
   * This is used to insert one point to KDtree 
   * 
   */
    def insert(value:Point3D):Unit={
       
      if(this.point==null){
         this.point=value
         this.box=BoxEnvelope.apply(0,4,0,3,0,7)
      }
     else
      insertElement(this,value,0)

         
    }

  /**
   * Print the content of the KDtree
   * 
   */
  def printKDtree(current:KDtree):Unit={
      if(current!=null){ 
        print(" Pointis: "+current.point.x+","+current.point.y+","+current.point.z+" level:"+current.level)
        println("  Xmin:"+current.box.minX + "  Xmax:"+current.box.maxX +"  ymin:"+current.box.minY +"  Ymax:"+current.box.maxY +"  Zmin:"+current.box.minZ +"  Zmax:"+current.box.maxZ )
        printKDtree(current.left)
        printKDtree(current.right)
      }
  }
   
}
