package com.lenovo.mqm.spark.move

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: gulei2
 * description: new move table generation
 * 1. move join newShipment on from_imei
 * 2. move join newShipment on to_imei
 * 3. prepend eventtype and eventdate columns
 * 
 */
object NewMove {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     
     val newShipmentInputPath = scala.Option(hadoopConf.get("newShipmentInputPath"))
     val moveInputPath = scala.Option(hadoopConf.get("moveInputPath"))
     val outputPath = scala.Option(hadoopConf.get("outputPath"))
     
     if (!newShipmentInputPath.isDefined || !moveInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: NewMove " +
                    "-DnewShipmentInputPath=<arg> " +
                    "-DmoveInputPath=<arg> " +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
      hdfs.delete(new Path(outputPath.get), true)
      
     val sparkConf = new SparkConf().setAppName("New Move Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val newShipmentLines = sc.textFile(newShipmentInputPath.get)
     // (imei, newShipmentArray)
     val newShipmentPair = newShipmentLines.map(s => s.split("\001", -1)).
     map (parts => (parts(9), parts))
     
     val moveLines = sc.textFile(moveInputPath.get)
     // (fromimei, moveArray)
     val movePair = moveLines.map(s => s.split("\001", -1)).
     map (parts => (parts(2), parts))
     // 1. move join newShipment on from_imei
     // movePair: (fromimei, moveArray)
     // newShipmentPair: (imei, newShipmentArray)
     // joined_1: (imei, (moveArray, newShipmentArray))
     val joined_1 = movePair.join(newShipmentPair)
     
     // (toimei, [newShipmentArray,moveArray,repairArray])
     val joined_1_map = joined_1.map {
        case (k, v) => (v._1(3), (v._2 ++ v._1 ++ Array.fill[String](39)("\\N")) )
     }
     
     // joined_1_map: (toimei, [newShipmentArray,moveArray,repairArray])
     // newShipmentPair: (imei, newShipmentArray)
     // joined_2: (imei, ([newShipmentArray,moveArray,repairArray], newShipmentArray))
     val joined_2 = joined_1_map.join(newShipmentPair)
     
     // 3. prepend eventtype and eventdate columns
     // [eventtype,eventdate,newShipmentArray,moveArray,repairArray,newShipmentArray]
     val newMove = joined_2.map {
       case (k, v) => (Array(v._1(48), v._1(49)) ++ v._1 ++ v._2).mkString("\001")
     }
     newMove.coalesce(6, true).saveAsTextFile(outputPath.get)
  }
}