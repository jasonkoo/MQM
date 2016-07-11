package com.lenovo.mqm.spark.cabandon

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/** 
 *  Author: gulei2
 *  description: filter out cabandon records not in shipment
 *  
 */
object CAbandonException {
   def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val cAbandonInputPath = scala.Option(hadoopConf.get("cAbandonInputPath"))
     val shipmentInputPath = scala.Option(hadoopConf.get("shipmentInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!cAbandonInputPath.isDefined || !shipmentInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: CAbandonException " +
                    "-DcAbandonInputPath=<arg> " +
                    "-DshipmentInputPath=<arg>" +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
       hdfs.delete(new Path(outputPath.get), true)
     
     
     val sparkConf = new SparkConf().setAppName("CAbandon Exception Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val cAbandonLines = sc.textFile(cAbandonInputPath.get)
     // (deviceid, cAbandonArray)
     val cAbandonPair = cAbandonLines.map { s => 
       val parts = s.split("\001", -1)
       (parts(2), parts)
     }
     
     val shipmentLines = sc.textFile(shipmentInputPath.get)
     // (deviceid, shipmentArray)
     val shipmentPair = shipmentLines.map { s => 
       val parts = s.split("\001", -1)
       (parts(9), parts)
     }
     
     // cAbandonPair: (deviceid, cAbandonArray)
     // shipmentPair: (deviceid, shipmentArray)
     // cAbandonSubtract: (deviceid, cAbandonArray)
     val cAbandonSubtract = cAbandonPair.subtractByKey(shipmentPair).map {
       case (k, v) => v.mkString("\001")
     }
     
     cAbandonSubtract.coalesce(10, true).saveAsTextFile(outputPath.get)
   }
}