package com.lenovo.mqm.spark.sdac

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Author: gulei2
 * description: sdac table generation
 * 1. sdac deduplication
 * 2. join shipment model 
 * 3. append first_sdac_date column
 * 
 */
object SDAC {
   def main(args: Array[String]) {
      val hadoopConf = new Configuration()
    new GenericOptionsParser(hadoopConf, args)
    val sdacOrigInputPath = scala.Option(hadoopConf.get("sdacOrigInputPath"))
    val shipmentModelInputPath = scala.Option(hadoopConf.get("shipmentModelInputPath"))   
    val outputPath = scala.Option(hadoopConf.get("outputPath"))
    
    if (!sdacOrigInputPath.isDefined  || !outputPath.isDefined) {
      System.err.println("usage: SDAC " +
                    "-DsdacOrigInputPath=<arg> " +
                    "-DshipmentModelInputPath=<arg> " +
                    "-DoutputPath=<arg>")
      System.exit(1)
    }
    
    val hdfs = FileSystem.get(hadoopConf)
    if (hdfs.exists(new Path(outputPath.get))) 
      hdfs.delete(new Path(outputPath.get), true)
      
    val sparkConf = new SparkConf().setAppName("SDAC Table Generation")
    val sc = new SparkContext(sparkConf)
    
    val shipmentModelLines = sc.textFile(shipmentModelInputPath.get)
    // (deviceid, model)
    val shipmentModelPair = shipmentModelLines.map { s => s.split("\001", -1) }.
        filter(parts => parts.length == 2).
        map(parts => (parts(0), parts(1)))
    
    val sdacLines = sc.textFile(sdacOrigInputPath.get)
    // 1. sdac deduplication 
    // (deviceid, (sdacArray, create_date))
    val sdacPair = sdacLines.map ( s => s.split("\001", -1) ).
       filter(parts => parts.length == 7 && parts(4).length == 10).
       map( parts => ( parts(1), (parts, parts(4))) ).
       foldByKey( (new Array[String](7), "2020-01-01") ) ((acc, element) => if (acc._2 > element._2) element else acc)
    
    // 2. join shipment model
    // sdacPair: (deviceid, (sdacArray, create_date))
    // shipmentModelPair: (deviceid, model)
    // joined_1: (deviceid, ((sdacArray, create_date), model))
    val joined_1 = sdacPair.join(shipmentModelPair)
    
    //  3. append first_sdac_date column
    // (model, first_sdac_date)
    val modelFSDPair = joined_1.map { case (k, v) => (v._2, v._1._2) }.
                     foldByKey("2020-01-01") ((acc, element) => if (acc > element) element else acc)
    val modelFSDMap = sc.broadcast(modelFSDPair.collectAsMap())
    
    // [sdacArray, first_sdac_date]
    val sdac = joined_1.map {
      case (k, v) => (v._1._1 :+ modelFSDMap.value.get(v._2).get).mkString("\001")
    }
     
    sdac.coalesce(80, true).saveAsTextFile(outputPath.get);  
  }
}