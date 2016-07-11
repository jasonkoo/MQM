package com.lenovo.mqm.spark.complex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: gulei2
 * description: new cabandon, dabandon, swap table generation
 * 1. exchange join newShipment
 * 2. prepend eventtype and eventdate columns
 * 
 */
object NewComplex {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     
     val newShipmentInputPath = scala.Option(hadoopConf.get("newShipmentInputPath"))
     val exchangeInputPaths = scala.Option(hadoopConf.get("exchangeInputPaths"))
     val outputPath = scala.Option(hadoopConf.get("outputPath"))
     
     if (!newShipmentInputPath.isDefined || !exchangeInputPaths.isDefined || !outputPath.isDefined) {
      System.err.println("usage: NewComplex " +
                    "-DnewShipmentInputPath=<arg> " +
                    "-DexchangeInputPaths=<arg> " +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
      hdfs.delete(new Path(outputPath.get), true)
      
     val sparkConf = new SparkConf().setAppName("New CAbandon, DAbandon, Swap Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val newShipmentLines = sc.textFile(newShipmentInputPath.get)
     // (deviceid, newShipmentArray)
     val newShipmentPair = newShipmentLines.map(s => s.split("\001", -1)).
     map (parts => (parts(9), parts))
     
     val exchangeLines = sc.textFile(exchangeInputPaths.get)
     // (deviceid, exchangeArray)
     val exchangePair = exchangeLines.map(s => s.split("\001", -1)).
     map (parts => (parts(2), parts))
     
     // 1. exchange join newShipment
      // exchangePair: (deviceid, exchangeArray)
      // newShipmentPair: (deviceid, newShipmentArray)
     // joined_1: (deviceid, (exchangeArray, newShipmentArray))
     val joined_1 = exchangePair.join(newShipmentPair)
     
     // 2. prepend eventtype and eventdate columns
     // eventtype,eventdate,newShipmentArray,exchangeArray
     val exhange = joined_1.map {
       case (k, v) => (Array(v._1(4), v._1(5)) ++ v._2 ++ v._1).mkString("\001")
     }
     
     exhange.coalesce(250, true).saveAsTextFile(outputPath.get)
  }
}