package com.lenovo.mqm.spark.move

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer


/*
 * Author: gulei2
 * description: move table generation
 * 1. deduplication
 * 2. aggregateByKey
 * 3. sortAndChain
 * 4. format and output
 */

object Move {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val ussDeviceOrigInputPath = scala.Option(hadoopConf.get("ussDeviceOrigInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!ussDeviceOrigInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: Move " +
                    "-DussDeviceOrigInputPath=<arg> " +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
       hdfs.delete(new Path(outputPath.get), true)
     
     val sparkConf = new SparkConf().setAppName("Move Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val ussDeviceLines = sc.textFile(ussDeviceOrigInputPath.get)
     // 1. deduplication
     // (imsi, (deviceid, devicemodel, thedate))
     val ussDevicePair = ussDeviceLines.map ( s => s.split("\001", -1) ).
     filter(parts => parts.length == 13 && parts(9).length > 10 && parts(10).length > 10).
     flatMap(parts => List( (parts(4), (parts(3), parts(7), parts(9).substring(0, 10))), (parts(4), (parts(3), parts(7), parts(10).substring(0, 10))) ) ).
     distinct()
     
     // 2. aggregateByKey
     // (imsi, [(deviceid, devicemodel, thedate)])
     val zero = new ArrayBuffer[(String, String, String)]()
     val aggregated = ussDevicePair.aggregateByKey(zero)(
       (array, v) => array += v,
       (array1, array2) => array1 ++= array2)
     
     // 3. sortAndChain
     // (imsi, (from_imei, to_imei, eventdate))
     val sortAndChained = aggregated.flatMapValues( v =>  sortAndChain(v) )
     
     // 4. format and output
     // (uid, uidtype, imei, toimei, eventtype, eventdate)
     val move = sortAndChained.map {
       case (k, v) => Array(k, "imsi", v._1, v._2, "move", v._3).mkString("\001")
     }
     move.coalesce(12, true).saveAsTextFile(outputPath.get)
  }
  
   // [(deviceid, devicemodel, thedate)]
  def sortAndChain(deviceArray: ArrayBuffer[(String, String, String)]): ArrayBuffer[(String, String, String)] = {
    val sortedDeviceArray = deviceArray.sortWith(_._3 < _._3)
    val result = new ArrayBuffer[(String, String, String)]()  
    // imei_l,imei_r,thedate_r
    for (i <- 0 to (sortedDeviceArray.length -2) ) {
      val l = sortedDeviceArray(i)
      val r = sortedDeviceArray(i + 1)
      if (l._1 != r._1 && l._2 != r._2 && l._3 != r._3) result += ((l._1, r._1, r._3))
    }
    result
  } 
}