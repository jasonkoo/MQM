package com.lenovo.mqm.spark.repair

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/** 
 *  Author: gulei2
 *  description: filter out repair records not in shipment
 *  
 */
object RepairException {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val repairInputPath = scala.Option(hadoopConf.get("repairInputPath"))
     val shipmentInputPath = scala.Option(hadoopConf.get("shipmentInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!repairInputPath.isDefined || !shipmentInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: RepairException " +
                    "-DrepairInputPath=<arg> " +
                    "-DshipmentInputPath=<arg>" +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
      hdfs.delete(new Path(outputPath.get), true)
     
     val sparkConf = new SparkConf().setAppName("Repair Exception Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val repairLines = sc.textFile(repairInputPath.get)
     // (deviceid, repairArray)
     val repairPair = repairLines.map ( s => s.split("\001", -1) ).
     filter(parts => parts.length == 37 && parts(13).length == 10).
     map(parts => ((parts(4), parts(13)), (parts, parts(13)))).
     foldByKey( (new Array[String](37), "2020-01-01") ) ((acc, element) => if (acc._2 > element._2) element else acc).
     map {
       case (k, v) => (k._1, v._1)
     }
     
     val shipmentLines = sc.textFile(shipmentInputPath.get)
     // (deviceid, dummy)
     val shipmentPair = shipmentLines.map { s => 
       val parts = s.split("\001", -1)
       (parts(9), "dummy")
     }
     
     // repairPair: (deviceid, repairArray)
     // shipmentPair: (deviceid, dummy)
     // repairSubtract: (deviceid, repairArray)
     val repairSubtract = repairPair.subtractByKey(shipmentPair).map {
       case (k, v) => v.mkString("\001")
     }
     repairSubtract.coalesce(20, true).saveAsTextFile(outputPath.get)
  }
}