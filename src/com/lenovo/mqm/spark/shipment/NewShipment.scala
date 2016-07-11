package com.lenovo.mqm.spark.shipment

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: gulei2
 * description: new shipment table generation
 * 1. shipment left outer join sdac
 * 2. left outer join activation
 * 3. append first_launch_date column
 * 4. filter out newShipment out of chronological order
 * 5. format and output new shipment for join
 * 6. prepend eventtype, eventdate columns
 * 7. output new shipment for union
 * 
 */
object NewShipment {
   def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val shipmentInputPath = scala.Option(hadoopConf.get("shipmentInputPath"))
     val sdacInputPath = scala.Option(hadoopConf.get("sdacInputPath"))
     val activationInputPath =  scala.Option(hadoopConf.get("activationInputPath"))
     val outputPathForJoin = scala.Option(hadoopConf.get("outputPathForJoin"))
     val outputPathForUnion = scala.Option(hadoopConf.get("outputPathForUnion"))
     val newShipmentExceptionOutputPath = scala.Option(hadoopConf.get("newShipmentExceptionOutputPath"))
     
     if (!shipmentInputPath.isDefined || !sdacInputPath.isDefined || !activationInputPath.isDefined 
         || !outputPathForJoin.isDefined || !outputPathForUnion.isDefined || !newShipmentExceptionOutputPath.isDefined) {
      System.err.println("usage: NewShipment " +
                    "-DshipmentInputPath=<arg> " +
                    "-DsdacInputPath=<arg> " +
                    "-DactivationInputPath=<arg> " +
                    "-DoutputPathForJoin=<arg> " +
                    "-DoutputPathForUnion=<arg> " +
                    "-DnewShipmentExceptionOutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPathForJoin.get))) 
      hdfs.delete(new Path(outputPathForJoin.get), true)
      
     if (hdfs.exists(new Path(outputPathForUnion.get))) 
      hdfs.delete(new Path(outputPathForUnion.get), true)
      
     if (hdfs.exists(new Path(newShipmentExceptionOutputPath.get)))
       hdfs.delete(new Path(newShipmentExceptionOutputPath.get), true)
      
     val sparkConf = new SparkConf().setAppName("New Shipment Table Generation")
     val sc = new SparkContext(sparkConf)
     
     val shipmentLines = sc.textFile(shipmentInputPath.get)
     // (deviceid, shipmentArray)
     val shipmentPair = shipmentLines.map(s => s.split("\001", -1)).
     map (parts => (parts(9), parts))
     
    
     val sdacLines = sc.textFile(sdacInputPath.get)
     // (deviceid, sdacArray)
     val sdacPair = sdacLines.map(s => s.split("\001", -1)).
     map (parts => (parts(1), parts))
     
    
     val activationLines = sc.textFile(activationInputPath.get)
     // (deviceid, activationArray)
     val activationPair = activationLines.map(s => s.split("\001", -1)).
     map (parts => (parts(0), parts))
     
     // 1. shipment left outer join sdac
     // shipmentPair: (deviceid, shipmentArray)
     // sdacPair: (deviceid, sdacArray)
     // joined_1: (deviceid, (shipmentArray, sdacArray))
     val joined_1 = shipmentPair.leftOuterJoin(sdacPair)
     
     
     // (deviceid, [shipmentArray, sdacArray])
     val joined_1_map = joined_1.map {
       case (k, v) => (k, v._1 ++ fillColumn(v._2, 8))
     }
     
     //val shipmentAndSdacSize = joined_1_map.count();
     //val activationSize = activationPair.count();
     //println("shipmentAndSdacSize: " + shipmentAndSdacSize + ", activationSize: " + activationSize)
     //System.exit(1)     
     //shipmentAndSdacSize: 106702039, activationSize: 23901483
     
     // 2. left outer join activation
     // joined_1_map: (deviceid, [shipmentArray, sdacArray])
     // activationPair: (deviceid, activationArray)
     // joined_2: (deviceid, ([shipmentArray, sdacArray], activationArray))
     val joined_2 = joined_1_map.leftOuterJoin(activationPair)
     
     // (deviceid, [shipmentArray, sdacArray, activationArray])
     val joined_2_map = joined_2.map {
       case (k, v) => (v._1 ++ fillColumn(v._2, 4))
     }
     
     // 3. append first_launch_date column
     // (model, first_sdac_date)
     val firstSDACDate = joined_2_map.map(v => (v(3), v(38))).
       foldByKey("2020-01-01") ((acc, element) => if (acc > element) element else acc)
     // (model, first_act_date)
     val firstActivationDate = joined_2_map.map (v => (v(3), v(41))).
       foldByKey("2020-01-01") ((acc, element) => if (acc > element) element else acc)
     // (model, (first_sdac_date, first_act_date))
     val firstLaunchDate = firstSDACDate.fullOuterJoin(firstActivationDate).map {
       case (k, v) => (k, minDate(noneTreatment(v._1), noneTreatment(v._2)))
     }
     
     val modelFLDMap = sc.broadcast(firstLaunchDate.collectAsMap())
     
     val newShipment = joined_2_map.map (v => (v :+ noneTreatment(modelFLDMap.value.get(v(3)))))
     
     // 4. filter out newShipment out of chronological order, i.e. shipment_date > min(sdac_date,activation_date)
     val newShipmentException = newShipment.filter { 
       v => !isInChronological(v(11), minDate(v(35), v(40))) }.
       map(v => v.mkString("\001"))
     newShipmentException.coalesce(10, true).saveAsTextFile(newShipmentExceptionOutputPath.get)
     
     // 5. format and output new shipment for join
     val newShipmentForJoin =  newShipment.map { v => v.mkString("\001") }
     newShipmentForJoin.coalesce(500, true).saveAsTextFile(outputPathForJoin.get)
     
     // 6. prepend eventtype, eventdate column
     val newShipmentForUnion = newShipment.map (v => (Array("shipment", v(11)) ++ v).mkString("\001"))
     
     // 7. output new shipment for union
     newShipmentForUnion.coalesce(500, true).saveAsTextFile(outputPathForUnion.get)
   }
   
   def fillColumn(x: Option[Array[String]], n: Int) = x match {
     case Some(s) => s
     case None => Array.fill[String](n)("\\N")
   }
   
   def noneTreatment(x: Option[String]) = x match {
     case Some(s) => s
     case None => "NA"
   }
   
   def minDate(date1: String, date2: String): String = {
     if (date1.length() == 10 && date2.length() == 10) {
       if (date1.compareTo(date2) > 0) date2 else date1
     } else if (date1.length() == 10 && date2.length() != 10) {
       date1
     } else if (date1.length() != 10 && date2.length() == 10) {
       date2
     } else {
       "NA"
     }
       
   }
   
    def isInChronological(earlierDate: String, laterDate: String): Boolean = {
    if (earlierDate.length() == 10 && laterDate.length() == 10) {
      if (earlierDate.compareTo(laterDate) <= 0) true else false 
    } else {
       true
    }   
   }
}