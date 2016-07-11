package com.lenovo.mqm.spark.repair

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: gulei2
 * description: new repair table generation
 * 1. repair join newShipment
 * 2. prepend eventtype and eventdate columns
 * 3. filter out newRepair out of chronological order
 * 
 */
object NewRepair {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     
     val newShipmentInputPath = scala.Option(hadoopConf.get("newShipmentInputPath"))
     val repairInputPath = scala.Option(hadoopConf.get("repairInputPath"))
     val outputPath = scala.Option(hadoopConf.get("outputPath"))
     val newRepairExceptionOutputPath = scala.Option(hadoopConf.get("newRepairExceptionOutputPath"))
     if (!newShipmentInputPath.isDefined || !repairInputPath.isDefined || !outputPath.isDefined || !newRepairExceptionOutputPath.isDefined) {
      System.err.println("usage: NewRepair " +
                    "-DnewShipmentInputPath=<arg> " +
                    "-DrepairInputPath=<arg> " +
                    "-DoutputPath=<arg> " +
                    "-DnewRepairExceptionOutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
      hdfs.delete(new Path(outputPath.get), true)
      
     if (hdfs.exists(new Path(newRepairExceptionOutputPath.get)))
       hdfs.delete(new Path(newRepairExceptionOutputPath.get), true)
      
     val sparkConf = new SparkConf().setAppName("New Repair Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val newShipmentLines = sc.textFile(newShipmentInputPath.get)
      // (deviceid, newShipmentArray)
     val newShipmentPair = newShipmentLines.map(s => s.split("\001", -1)).
     map (parts => (parts(9), parts))
     
     val repairLines = sc.textFile(repairInputPath.get)
     // (deviceid, repairArray)
     val repairPair = repairLines.map ( s =>  s.split("\001", -1)).
     map (parts => (parts(4), parts))
     
     // 1. repair join newShipment
     // repairPair: (deviceid, repairArray)
     // newShipmentPair: (deviceid, newShipmentArray)
     // joined_1: (deviceid, (repairArray, newShipmentArray))
     val joined_1 = repairPair.join(newShipmentPair)
     
     joined_1.cache()
     
     // 2. prepend eventtype and eventdate columns
     // [eventtype,eventdate,newShipmentArray,exchangeArray,repairArray]
     val newRepair = joined_1.map {
       case (k, v) => (Array("repair", v._1(13)) ++ v._2 ++ Array.fill[String](6)("\\N") ++ v._1).mkString("\001")
     }
     newRepair.coalesce(42, true).saveAsTextFile(outputPath.get)
     
     // 3. filter out newRepair out of chronological order
     val newRepairException = joined_1.filter(v => !isInChronological(minDate(v._2._2(35), v._2._2(40)), v._2._1(13))).map {
       case (k, v) => (v._2 ++ v._1).mkString("\001")
     }
     newRepairException.coalesce(10, true).saveAsTextFile(newRepairExceptionOutputPath.get)
     
  }
  
  def minDate(date1: String, date2: String): String = {
     if (date1.length() == 10 && date2.length() == 10) {
       if (date1.compareTo(date2) > 0) date2 else date1
     } else if (date1.length() == 10 && date2.length() != 10) {
       date1
     } else if (date1.length() != 10 && date2.length() == 10) {
       date2
     } else {
       "NA"
     }
       
   }
   
    def isInChronological(earlierDate: String, laterDate: String): Boolean = {
      if (earlierDate.length() == 10 && laterDate.length() == 10) {
        if (earlierDate.compareTo(laterDate) <= 0) true else false 
      } else {
         true
      }   
   }
}