package com.lenovo.mqm.spark.repair

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import java.util.Date
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

/**
 *  Author: gulei2
 *  description: repair table generation
 *  1. deduplication
 *  2. append failure_count column
 *  3. append shipment_date column
 *  4. append is_real_in_warranty column
 * 
 */
object Repair {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     val repairInputPath = scala.Option(hadoopConf.get("repairInputPath"))
     val shipmentOrigInputPath = scala.Option(hadoopConf.get("shipmentOrigInputPath"))
     val outputPath =  scala.Option(hadoopConf.get("outputPath"))
     
     if (!repairInputPath.isDefined || !outputPath.isDefined) {
       System.err.println("usage: Repair " +
                    "-DrepairInputPath=<arg> " +
                    "-DshipmentOrigInputPath=<arg>" +
                    "-DoutputPath=<arg>")
       System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
       hdfs.delete(new Path(outputPath.get), true)
     
     val sparkConf = new SparkConf().setAppName("Repair Table Geneartion")
     val sc = new SparkContext(sparkConf)
     
     val repairLines = sc.textFile(repairInputPath.get)
     // 1. deduplication
     // (deviceid, repairArray)
     val repairPair = repairLines.map { s => s.split("\001", -1)  }.
     filter(parts => parts.length == 37 && parts(13).length == 10).
     map(parts => ((parts(4), parts(13)), (parts, parts(13)))).
     foldByKey( (new Array[String](37), "2020-01-01") ) ((acc, element) => if (acc._2 > element._2) element else acc).
     map {
       case (k, v) => (k._1, v._1)
     }
     
     // 2. append failure_cnt column     
     val zero = new ArrayBuffer[Array[String]]()  
     // (deviceid, [repairArray,failure_count])
     val failureCntPair = repairPair.aggregateByKey(zero)(
       (array, v) => array += v,
       (array1, array2) => array1 ++= array2).
       flatMapValues(v => sortAndTag(v))
     // 3. append shipment_date column
     val shipmentLines = sc.textFile(shipmentOrigInputPath.get)
     // (deviceid, shipment_date)
     val shipmentPair = shipmentLines.map { s => s.split("\001", -1) }.
       filter(parts => parts.length == 29 && parts(11).length == 10).
       map( parts => (parts(9), parts(11)) ).
       foldByKey( "2000-01-01" ) ((acc, element) => if (acc > element) acc else element)
       
     // failureCntPair: (deviceid, [repairArray,failure_count])
     // shipmentPair: (deviceid, shipment_date)
     // joined_1: (deviceid, (repairArray, shipment_date))
     val joined_1 = failureCntPair.join(shipmentPair)
     
     // 4. append is_real_in_warranty column
     // 0: data souce exception 1: in warranty 2: out of warranty 3: warrantyPeriod < 0  4: shipmentdate or claimdate format exception 
     // repairArray, failure_count, is_real_in_warranty
     val repair = joined_1.map (v => addWarrantyTag(v._2._1, v._2._2).mkString("\001") )
     repair.coalesce(20, true).saveAsTextFile(outputPath.get)
     
  }
  def sortAndTag(repairArray: ArrayBuffer[Array[String]]) : ArrayBuffer[Array[String]] = {
    val sorted = repairArray.sortWith(_(13) < _(13))
    val result = new ArrayBuffer[Array[String]]()
    result += sorted(0) :+ "1"
    for(i <- 1 to (sorted.length -1) ) {
      result += sorted(i) :+ "2"
    }
    result
  }
  
  def addWarrantyTag(repairArray: Array[String], shipmentDate: String) : Array[String] = {
    val domesticBucket = HashSet("Lenovo PRC SIM Repair", "Lenovo PRC SIM DOA_DAP")
    val overseasBucket = HashSet("Lenovo TB ROW JD", "Lenovo SP ROW BW", "Lenovo SP ROW Manual", "Lenovo TB ROW Manual (DOA)", "Lenovo TB ROW Manual (Repair)" )
    val dataSource = repairArray(0)
    val purchaceDate = repairArray(14)
    val claimDate = repairArray(13)
    val warrantyPeriod = repairArray(27).toInt
    if (warrantyPeriod < 0 ) {
      repairArray :+ "3"
    } else {
       if (domesticBucket.contains(dataSource)) {
         if (purchaceDate.length() == 10) {
          repairArray :+ withinPeriodTest(purchaceDate, claimDate, warrantyPeriod)
         } else {
            repairArray :+ withinPeriodTest(shipmentDate, claimDate, warrantyPeriod + 3)
         }
      } else if (overseasBucket.contains(dataSource)) {
        repairArray :+ withinPeriodTest(shipmentDate, claimDate, warrantyPeriod + 3)
      } else {
        repairArray :+ "0"
      }
    }
  }
  
  def withinPeriodTest(startDate: String, endDate: String, period: Int): String = {
     if (startDate.length() == 10 && endDate.length() == 10 && period > 0) {
       val formatter = new SimpleDateFormat("yyyy-MM-dd")
       val start = formatter.parse(startDate)
       val end = formatter.parse(endDate)
       val diff = end.getTime - start.getTime
       val tu = TimeUnit.DAYS
       if (tu.convert(diff, TimeUnit.MILLISECONDS) <= period * 30) {
         "1"
       } else {
         "2"
       }
     } else {
       "4"
     }
  }
  
  def isNull(x: Array[String]) = Option(x) match {
     case Some(s) => false
     case None => true
  }  
}