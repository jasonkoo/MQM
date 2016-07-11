package com.lenovo.mqm.spark.dabandon

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Date
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

/*
 * Author: gulei2
 * description: dabandon table generation
 * 1. deduplication
 * 2. filter
 * 3. format and output
 */
object DAbandon {
   def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val ussDeviceOrigInputPath = scala.Option(hadoopConf.get("ussDeviceOrigInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!ussDeviceOrigInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: DAbandon " +
                    "-DussDeviceOrigInputPath=<arg> " +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
       hdfs.delete(new Path(outputPath.get), true)
     
     
     val sparkConf = new SparkConf().setAppName("DAbandon Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val ussDeviceLines = sc.textFile(ussDeviceOrigInputPath.get)
     // 1. deduplication 
     // (deviceid, eventdate)
     val ussDevicePair = ussDeviceLines.map ( s => s.split("\001", -1) ).
     filter(parts => parts.length == 13 && parts(10).length > 10).
     map(parts => ( parts(3), parts(10).substring(0, 10) ) ).
     foldByKey("2000-01-01") ((acc, element) => if (acc > element) acc else element).
     // 2. filter
     filter{ case (k, v) =>  isOldEnough(v) }
     
     // 3. format and output
     // (uid, uidtype, imei, toimei, eventtype, eventdate)
     val dabandon = ussDevicePair.map {
       case (k, v) => Array("NA", "NA", k, "NA", "dabandon", v).mkString("\001")
     }
     dabandon.coalesce(30, true).saveAsTextFile(outputPath.get)
   }
   
   def isOldEnough(thedate: String): Boolean = {
     val now = new Date()
     val formatter = new SimpleDateFormat("yyyy-MM-dd")
     val someDate = formatter.parse(thedate)
     val diffInMillies = now.getTime() - someDate.getTime()
     val tu = TimeUnit.DAYS
     tu.convert(diffInMillies, TimeUnit.MILLISECONDS) > 28
   }
}