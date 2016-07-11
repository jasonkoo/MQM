package com.lenovo.mqm.spark.activation


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: gulei2
 * description: activation table generation
 * 1. deduplication
 * 2. append first_checkin_date column
 * 3. append activation_cnt column
 * 4. format and output
 */
object Activation {
   def main(args: Array[String]) {
    val hadoopConf = new Configuration()
    new GenericOptionsParser(hadoopConf, args)
    val ussDeviceOrigInputPath = scala.Option(hadoopConf.get("ussDeviceOrigInputPath"))
    val shipmentModelInputPath = scala.Option(hadoopConf.get("shipmentModelInputPath"))    
    val outputPath =  scala.Option(hadoopConf.get("outputPath"))
    
    if (!ussDeviceOrigInputPath.isDefined || !shipmentModelInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: Activation " +
                    "-DussDeviceOrigInputPath=<arg> " +
                    "-DshipmentModelInputPath=<arg>" +
                    "-DoutputPath=<arg>")
      System.exit(1)
    } 
    
    val hdfs = FileSystem.get(hadoopConf)
    if (hdfs.exists(new Path(outputPath.get))) 
       hdfs.delete(new Path(outputPath.get), true)
     
    val sparkConf = new SparkConf().setAppName("Activation Table Generation")
    val sc = new SparkContext(sparkConf)
    
    val shipmentModelLines = sc.textFile(shipmentModelInputPath.get)
    // (imei, model)
    val shipmentModelPair = shipmentModelLines.map { s => s.split("\001", -1) }.
        filter(parts => parts.length == 2).
        map(parts => (parts(0), parts(1)))
        
    val ussDeviceLines = sc.textFile(ussDeviceOrigInputPath.get)
    //1. deduplication
    // (deviceid, (imsi, activation_date))
    val ussDevicePair = ussDeviceLines.map ( s => s.split("\001", -1) ).
     filter(parts => parts.length==13 && parts(10).length > 10).
     map(parts => (parts(3), (parts(4), parts(10).substring(0, 10)))).
     foldByKey(("dummy", "2000-01-01")) ((acc, element) => if (acc._2 > element._2) acc else element)
     
    // 2. append first_checkin_date column
    // ussDevicePair: (deviceid, (imsi, activation_date))
    // shipmentModelPair: (deviceid, model)
    // joined_1: (deviceid, ((imsi, activation_date), model))
    val joined_1 = ussDevicePair.join(shipmentModelPair)
    
     // (model, first_checkin_date)
    val modelFCDPair = joined_1.map { case (k, v) => (v._2, v._1._2) }.
                     foldByKey("2020-01-01") ((acc, element) => if (acc > element) element else acc)
    val modelFCDMap = sc.broadcast(modelFCDPair.collectAsMap())
    
    // joined_1: (deviceid, ((imsi, activation_date), model))
    // modelFCDMap: (model, first_checkin_date)
    // joined_2: (deviceid, (imsi, activation_date, model, first_checkin_date))
    val joined_2 = joined_1.map {
      case (k, v) => (k, (v._1._1, v._1._2, v._2, modelFCDMap.value.get(v._2).get))
    }
    
    // 3. append activation_cnt column
    // (deviceid, checkin_count) 如果一个IMEI只有一个IMSI则此值为1，否则为2
    val checkinCntPair = ussDevicePair.map {
     case (k, v) => (k, v._1)
    }.distinct().map {
      case (k, v) => (k, 1)
    }.reduceByKey(_ + _).map {
      case (k, v) => (k, if (v == 1) 1 else 2)
    }
    
    // joined_2: (deviceid, (imsi, activation_date, model, first_checkin_date))
    // checkinCntPair: (deviceid, checkin_count) 
    // joined_3: (deviceid, ((imsi, activation_date, model, first_checkin_date), checkin_count))
    val joined_3 = joined_2.join(checkinCntPair)
    
    // 4. format and output
    // deviceid,activation_date,first_checkin_date,checkin_count
    val activation = joined_3.map {
      case (k, v) => Array(k, v._1._2, v._1._4, v._2).mkString("\001")
    }
    activation.coalesce(10, true).saveAsTextFile(outputPath.get);
   }
}