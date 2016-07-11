package com.lenovo.mqm.spark.cabandon

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
 * description: cabandon table generation
 * 1. deduplication
 * 2. filter
 * 3. format and output
 */

object CAbandon {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val ussDeviceOrigInputPath = scala.Option(hadoopConf.get("ussDeviceOrigInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!ussDeviceOrigInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: CAbandon " +
                    "-DussDeviceOrigInputPath=<arg> " +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
       hdfs.delete(new Path(outputPath.get), true)
     
     val sparkConf = new SparkConf().setAppName("CAbandon Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val ussDeviceLines = sc.textFile(ussDeviceOrigInputPath.get)
      // 1. deduplication 
     // (imsi, (deviceid, eventdate))
     val ussDevicePair = ussDeviceLines.map ( s => s.split("\001", -1) ).
     filter(parts => parts.length == 13 && parts(10).length > 10).
     map(parts => ( parts(4), (parts(3), parts(10).substring(0, 10)) ) ).
     foldByKey( ("dummy", "2000-01-01") ) ((acc, element) => if (acc._2 > element._2) acc else element).
     // 2. filter
     filter{ case (k, v) =>  isOldEnough(v._2) }
     
     // 3. format and output
     // (uid, uidtype, imei, toimei, eventtype, eventdate)
     val cabandon = ussDevicePair.map {
       case (k, v) => Array(k, "imsi", v._1, "NA", "cabandon", v._2).mkString("\001")
     }
     cabandon.coalesce(30, true).saveAsTextFile(outputPath.get)
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