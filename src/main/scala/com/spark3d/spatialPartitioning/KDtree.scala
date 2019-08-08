/*
 * Copyright 2019 AstroLab Software
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
 * Printing the tree
 * Breadth-First Search to determine the bounding boxes for partitioning
 * */
 
class KDtree() extends Serializable {
  /**
   * @param point  Value of Node's  (x,y,z)
   * @param  box  Boundary box of the tree rooted at this node 
   * @param level The level of the node compared to the root of original tree
   * @param parent KDtree reference to the parent node  of the current node 
   * @param left KDtree reference to the left node of the current node 
   * @param parent KDtree reference to the right node of the current node 
   */

  //Initialization
  var point:Point3D=null
  var box: BoxEnvelope=null
  var level: Int=0
  var parent: KDtree = null
  var left: KDtree = null
  var right: KDtree = null
     
  /**
   * Insert an input object.3D point into the KDtree to build it
   * @param current KDtree root
   * @param value Input object/node (x,y,z)
   * @param level The depth of the node compared to the root of original tree
   * @return KDtree
   */
  def insertElement(current:KDtree, value:Point3D, level:Int):KDtree={
     
    //To determine the dimension (x,y or z)
     var currentLevel=level%3
      
     // For x dimension
     if(currentLevel==0){
       //left subtree
       if(value.x<current.point.x){
         if(current.left!=null ){
           insertElement(current.left,value,level+1)
          }
          else { //Creating left KDtree node in  x dimension
            current.left=new KDtree()
            current.left.point=value
            current.left.parent=current
            current.left.level=level+1
            current.left.box=BoxEnvelope.apply(current.box.minX,current.point.x,current.box.minY,current.box.maxY,current.box.minZ,current.box.maxZ)
         }
       }
       //right subtree
       else { 
           if(current.right!=null ){
              insertElement(current.right,value,level+1)
            }
            else { //Creating right KDtree node in x dimension
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
       //left subtree
           if(value.y<current.point.y){
               if(current.left!=null ){
                  insertElement(current.left,value,level+1)
                }
               else { //Creating left KDtree node in y dimension
                   current.left=new KDtree()
                   current.left.point=value
                   current.left.parent=current
                   current.left.level=level+1
                   current.left.box=BoxEnvelope.apply(current.box.minX,current.box.maxX,current.box.minY,current.point.y,current.box.minZ,current.box.maxZ)
                 }
           }
       //right subtree
         else {
             if(current.right!=null ){
                insertElement(current.right,value,level+1)
            }
            else { //Creating right KDtree node in y dimension
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
        //left subtree
            if(value.z<current.point.z){
                if(current.left!=null ){
                   insertElement(current.left,value,level+1)
                 }
                  else { //Creating left KDtree node in z dimension
                     current.left=new KDtree()
                     current.left.point=value
                     current.left.parent=current
                     current.left.level=level+1
                     current.left.box=BoxEnvelope.apply(box.minX,current.box.maxX,current.box.minY,current.box.maxY,current.box.minZ,current.point.z)
          }
        }
           //right subtree
           else {
               if(current.right!=null ){
                  insertElement(current.right,value,level+1)
             }
             else { //Creating right KDtree node in z dimension
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
   * This is used to add the new object/node to KDtree
   * Also it prepares the first node(root) with it's boundary box (Initialization node)
   * @param value  node's point(x,y,z)/input object
   * @param initialBox Enclosing box for the data set.
   */
  def insert( value:Point3D, initialBox:BoxEnvelope):Unit={
     // Initialize the root and the  boundary box for the dataset
      if(this.point==null){ 
        this.point=value
        this.box=initialBox
      } 
      //
     else
      insertElement(this,value,0)   
    }

    /**
     * This method receives list of 3D points, and it uses median technique to help insertElement method to build balanced KDtree
     * @param points List of 3D points
     * @param level The depth of the node compared to the root of original tree
     * @param initialBox Enclosing box for the data set.
     */
    def insertList ( points: List[Point3D], level:Int, initialBox:BoxEnvelope ):Unit  ={
      if( points!=null)
      {   
         var sortedPoints:List[Point3D] =List()
         var currentLevel=level%3
         //Ascending sorting based on the x coordinate.
         if(currentLevel == 0)
            {
              sortedPoints=points.sortWith(_.x<_.x)
            
            }
            //Ascending sorting based on the y coordinate
          else if(currentLevel == 1){
              sortedPoints=points.sortWith(_.y<_.y)
              
             }
             //Ascending sorting based on the z coordinate
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
   * @param current KDtree root
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
   * Applying Breadth-First Search (BFS) to determine the boundary boxes for partitioning
   * @param current KDtree root
   * @param level The depth of the node compared to the root of original tree
   * @return ListBuffer[BoxEnvelope]
   */
   var partitionBoundary= new ListBuffer[BoxEnvelope]
   var partitionID=0
   def BFS(current:KDtree,level:Int):ListBuffer[BoxEnvelope]={
     if(current==null)
     null
     else  if(level==1){
              current.box.indexID=partitionID
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
