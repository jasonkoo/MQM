package com.lenovo.mqm.spark.dabandon

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/** 
 *  Author: gulei2
 *  description: filter out dabandon records not in shipment
 *  
 */
object DAbandonException {
   def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val dAbandonInputPath = scala.Option(hadoopConf.get("dAbandonInputPath"))
     val shipmentInputPath = scala.Option(hadoopConf.get("shipmentInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!dAbandonInputPath.isDefined || !shipmentInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: DAbandonException " +
                    "-DdAbandonInputPath=<arg> " +
                    "-DshipmentInputPath=<arg>" +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
       hdfs.delete(new Path(outputPath.get), true)
     
     
     val sparkConf = new SparkConf().setAppName("DAbandon Exception Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val dAbandonLines = sc.textFile(dAbandonInputPath.get)
     // (deviceid, dAbandonArray)
     val dAbandonPair = dAbandonLines.map { s => 
       val parts = s.split("\001", -1)
       (parts(2), parts)
     }
     
     val shipmentLines = sc.textFile(shipmentInputPath.get)
     // (deviceid, shipmentArray)
     val shipmentPair = shipmentLines.map { s => 
       val parts = s.split("\001", -1)
       (parts(9), parts)
     }
     
     // dAbandonPair: (deviceid, dAbandonArray)
     // shipmentPair: (deviceid, shipmentArray)
     // dAbandonSubtract: (deviceid, dAbandonArray)
     val dAbandonSubtract = dAbandonPair.subtractByKey(shipmentPair).map {
       case (k, v) => v.mkString("\001")
     }
     
     dAbandonSubtract.coalesce(6, true).saveAsTextFile(outputPath.get)
   }
}