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

import com.astrolabsoftware.spark3d.spatialPartitioning
import com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope
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
            current.left.box=BoxEnvelope.apply(current.box.minX,current.point.x,current.box.minY,current.box.maxY,current.box.minZ,current.box.maxZ)
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
                 current.right.box=BoxEnvelope.apply(current.point.x,current.box.maxX,current.box.minY,current.box.maxY,current.box.minZ,current.box.maxZ)

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
                   current.left.box=BoxEnvelope.apply(current.box.minX,current.box.maxX,current.box.minY,current.point.y,current.box.minZ,current.box.maxZ)
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
                 current.right.box=BoxEnvelope.apply(current.box.minX,current.box.maxX,current.point.y,current.box.maxY,current.box.minZ,current.box.maxZ)
                 
                  }
       }   
     }// end of y dim

      // For z dimension
      else if(currentLevel==2){
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
                     current.left.box=BoxEnvelope.apply(box.minX,current.box.maxX,current.box.minY,current.box.maxY,current.box.minZ,current.point.z)
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
                  current.right.box=BoxEnvelope.apply(current.box.minX,current.box.maxX,current.box.minY,current.box.maxY,current.point.z,current.box.maxZ)
                  }
        }   
      }// end of z dim
      current
  } //end of inserElement mehod
  
  /**
   * This is used to insert one point to KDtree 
   * 
   */
  def insert( value:Point3D, initialBox:BoxEnvelope):Unit={
    
      if(this.point==null){ 
        this.point=value
        this.box=initialBox
      }
     else
      insertElement(this,value,0)
         
    }

    /**
     * Insert list of 3D points
     * 
     */
    def insertList ( points: List[Point3D], level:Int, initialBox:BoxEnvelope ):Unit  ={
      if( points!=null)
      {   
         var sortedPoints:List[Point3D] =List()
         var currentLevel=level%3

         if(currentLevel == 0)
            {
              sortedPoints=points.sortWith(_.x<_.x)
            
            }
          else if(currentLevel == 1){
              sortedPoints=points.sortWith(_.y<_.y)
              
             }
               else  
               sortedPoints=points.sortWith(_.z<_.z)
          

                
          var medianIndex:Int=(sortedPoints.length)/2 
          insert(sortedPoints(medianIndex),initialBox)
          var leftSubtree:  List[Point3D]=List()
          var rightSubtree: List[Point3D]=List()
          leftSubtree =sortedPoints.take(medianIndex)
          rightSubtree =sortedPoints.takeRight(sortedPoints.length-(medianIndex+1))  
        
          if(medianIndex-1>=0 &&leftSubtree.length>0){
                insertList(leftSubtree,level+1,initialBox) 
            }
       
         if(medianIndex-1<sortedPoints.length &&rightSubtree.length>0){
              insertList(rightSubtree,level+1,initialBox)
          }

       }//end points!=null
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

  /**
   * Breadth-First Search (BFS) and it determines the boundary boxes for partitioning
   */
   var partitionBoundary= new ListBuffer[BoxEnvelope]
   var partitionID=0
   def BFS(current:KDtree,level:Int):ListBuffer[BoxEnvelope]={
     if(current==null)
     null
     else  if(level==1){
              current.box.indexID=partitionID
              println(current.box.indexID)
              partitionBoundary += current.box
              partitionID+=1
            }
           else  if(level>1){
                 BFS(current.left,level-1)
                 BFS(current.right,level-1) 
           }
    partitionBoundary
   }
   
}
