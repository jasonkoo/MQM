package com.lenovo.mqm.spark.sdac

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: gulei2
 * description: filter out SDAC records not in shipment
 * 
 * 
 */
object SDACException {
   def main(args: Array[String]) {
      val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val SDACInputPath = scala.Option(hadoopConf.get("SDACInputPath"))
     val shipmentInputPath = scala.Option(hadoopConf.get("shipmentInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!SDACInputPath.isDefined || !shipmentInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: SDACException " +
                    "-DSDACInputPath=<arg> " +
                    "-DshipmentInputPath=<arg>" +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val sparkConf = new SparkConf().setAppName("SDAC Exception Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val SDACLines = sc.textFile(SDACInputPath.get)
     // (deviceid, SDACArray)
     val SDACPair = SDACLines.map ( s => s.split("\001", -1) ).
     filter(parts => parts.length == 7 && parts(4).length == 10).
     map( parts => ( parts(1), (parts, parts(4))) ).
     foldByKey( (new Array[String](7), "2020-01-01") ) ((acc, element) => if (acc._2 > element._2) element else acc).
     map {
       case (k, v) => (k, v._1)
     }
     
     val shipmentLines = sc.textFile(shipmentInputPath.get)
     // (deviceid, dummy)
     val shipmentPair = shipmentLines.map { s => 
       val parts = s.split("\001", -1)
       (parts(9), "dummy")
     }
     
     // SDACPair: (deviceid, SDACArray)
     // shipmentPair: (deviceid, dummy)
     // SDACSubtract: (deviceid, SDACArray)
     val SDACSubtract = SDACPair.subtractByKey(shipmentPair).map {
       case (k, v) => v.mkString("\001")
     }
     SDACSubtract.coalesce(20, true).saveAsTextFile(outputPath.get)
   }
}