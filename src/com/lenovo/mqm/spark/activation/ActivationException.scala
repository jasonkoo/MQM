package com.lenovo.mqm.spark.activation

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/** 
 *  Author: gulei2
 *  description: filter out activation records not in shipment
 *  
 */
object ActivationException {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val activationInputPath = scala.Option(hadoopConf.get("activationInputPath"))
     val shipmentInputPath = scala.Option(hadoopConf.get("shipmentInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!activationInputPath.isDefined || !shipmentInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: ActivationException " +
                    "-DactivationInputPath=<arg> " +
                    "-DshipmentInputPath=<arg>" +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val sparkConf = new SparkConf().setAppName("Activation Exception Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val activationLines = sc.textFile(activationInputPath.get)
     // (deviceid, activationArray)
     val activationPair = activationLines.map ( s => s.split("\001", -1) ).
     filter(parts => parts.length==13 && parts(10).length > 10).
     map(parts => (parts(3), (parts, parts(10).substring(0, 10)))).
     foldByKey((new Array[String](13), "2000-01-01")) ((acc, element) => if (acc._2 > element._2) acc else element).
     map {
       case (k, v) => (k, v._1)
     }
     
     val shipmentLines = sc.textFile(shipmentInputPath.get)
     // (deviceid, dummy)
     val shipmentPair = shipmentLines.map { s => 
       val parts = s.split("\001", -1)
       (parts(9), "dummy")
     }
     
     // activationPair: (deviceid, activationArray)
     // shipmentPair: (deviceid, dummy)
     // activationSubtract: (deviceid, activationArray)
     val activationSubtract = activationPair.subtractByKey(shipmentPair).map {
       case (k, v) => v.mkString("\001")
     }
     activationSubtract.coalesce(20, true).saveAsTextFile(outputPath.get)
  }
}