package com.lenovo.mqm.spark.shipment

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: gulei2
 * description: shipment table generation
 * 0. filter out shipment out of chronological order
 * 1. deduplication
 * 2. append first_mfg_date column
 * 3. append first_shipment_date column
 * 4. format and output shipment table
 * 5. output (imei, model) pair for sdac and activation
 * 
 */
object Shipment {
  def main(args: Array[String]) {
    val hadoopConf = new Configuration()
    new GenericOptionsParser(hadoopConf, args)
    val shipmentOrigInputPath = scala.Option(hadoopConf.get("shipmentOrigInputPath"))
    val outputPath = scala.Option(hadoopConf.get("outputPath"))
    val shipmentModelOutputPath =  scala.Option(hadoopConf.get("shipmentModelOutputPath"))
    val shipmentExceptionOutputPath = scala.Option(hadoopConf.get("shipmentExceptionOutputPath"))
    
    if (!shipmentOrigInputPath.isDefined || !outputPath.isDefined || !shipmentModelOutputPath.isDefined || !shipmentExceptionOutputPath.isDefined) {
      System.err.println("usage: Shipment " +
                    "-DshipmentOrigInputPath=<arg> " +
                    "-DoutputPath=<arg> " +
                    "-DshipmentModelOutputPath=<arg> " +
                    "-DshipmentExceptionOutputPath=<arg>")
      System.exit(1)
    }
    
    val hdfs = FileSystem.get(hadoopConf)
    if (hdfs.exists(new Path(shipmentModelOutputPath.get))) 
      hdfs.delete(new Path(shipmentModelOutputPath.get), true)
      
    if (hdfs.exists(new Path(outputPath.get))) 
      hdfs.delete(new Path(outputPath.get), true)
      
    if (hdfs.exists(new Path(shipmentExceptionOutputPath.get)))
      hdfs.delete(new Path(shipmentExceptionOutputPath.get), true)
      
    val sparkConf = new SparkConf().setAppName("Shipment Table Generation")
    val sc = new SparkContext(sparkConf)
      
    val shipmentLines = sc.textFile(shipmentOrigInputPath.get)
    
 
    // (model, first_mfg_date)
    val modelFMDPair = shipmentLines.map { s => s.split("\001", -1) }.
      filter(parts => parts.length == 29 && parts(4).length == 10).
      map( parts => (parts(3), parts(4)) ).
      foldByKey("2020-01-01") ((acc, element) => if (acc > element) element else acc)
    val modelFMDMap = sc.broadcast(modelFMDPair.collectAsMap())
    
    // (model, first_shipment_date)
    val modelFSDPair = shipmentLines.map { s => s.split("\001", -1) }.
      filter(parts => parts.length == 29 && parts(11).length == 10).
      map( parts => (parts(3), parts(11)) ).
      foldByKey("2020-01-01") ((acc, element) => if (acc > element) element else acc)
    val modelFSDMap = sc.broadcast(modelFSDPair.collectAsMap())
    
    // 0. filter out shipment out of chronological order, i.e. mfg_date > shipmentdate
    val shipmentException = shipmentLines.map { s => s.split("\001", -1) }.
    filter(parts => parts.length == 29 && !isInChronological(parts(4), parts(11))).
    map(v => v.mkString("\001"))
    shipmentException.coalesce(5, true).saveAsTextFile(shipmentExceptionOutputPath.get)
    
    // 1.  deduplication
    // (deviceid, ([shipmentArray], shipment_date))
    val shipmentPair = shipmentLines.map { s => s.split("\001", -1) }.
       filter(parts => parts.length == 29 && parts(11).length == 10).
       map( parts => (parts(9), (parts, parts(11))) ).
       foldByKey( (new Array[String](29), "2000-01-01") ) ((acc, element) => if (acc._2 > element._2) acc else element)
    
    // 2. append first_mfg_date column
    // (model,[shipmentArray])
    val modelPair = shipmentPair.map {
         case (k, v) => (v._1(3), v._1)
    }
    
    // modelPair: (model,[shipmentArray])
    // modelFMDMap: (model, first_mfg_date)
    // joined_1: (model, [shipmentArray, first_mfg_date])
    // Simply join modelPair and modelFMDPair will cause out of memeory exception, use broadcast variable instead
    val joined_1 = modelPair.map {
      case (k, v) => (k, v :+ noneTreatment(modelFMDMap.value.get(k)))
    }
    
    // 3. append first_shipment_date column
    // joined_1: (model, [shipmentArray, first_mfg_date])
    // modelFSDMap: (model, first_shipment_date)
    // joined_2: ([shipmentArray, first_mfg_date, first_shipment_date])   
    val joined_2 = joined_1.map {
      case (k, v) => (v :+ modelFSDMap.value.get(k).get)
    }
    
    // 4. format and output shipment table
    // shipmentArray,first_mfg_date,first_shipment_date
    val shipment = joined_2.map { v => v.mkString("\001") }
    shipment.coalesce(350, true).saveAsTextFile(outputPath.get)
    
    // 5. output (imei, model) pair for sdac and activation
    // (deviceid, model)
    val shipmentModel = shipmentPair.map {
      case (k, v) => Array(k, v._1(3)).mkString("\001")
    }
    shipmentModel.coalesce(64, true).saveAsTextFile(shipmentModelOutputPath.get)
  }
  
  def noneTreatment(x: Option[String]) = x match {
     case Some(s) => s
     case None => "NA"
  }
  
 def isInChronological(earlierDate: String, laterDate: String): Boolean = {
    if (earlierDate.length() == 10 && laterDate.length() == 10) {
      if (earlierDate.compareTo(laterDate) <= 0) true else false 
    } else {
       true
    }
 }
}