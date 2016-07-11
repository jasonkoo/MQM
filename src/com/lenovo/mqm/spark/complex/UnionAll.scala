package com.lenovo.mqm.spark.complex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: gulei2
 * description: union newshipment, newcomplex, newmove, newrepair table
 * 
 * 
 */
object UnionAll {
  def main(args: Array[String]) {
     val hadoopConf = new Configuration()
     new GenericOptionsParser(hadoopConf, args)
     
     val newShipmentInputPath = scala.Option(hadoopConf.get("newShipmentInputPath"))
     val newComplexInputPath = scala.Option(hadoopConf.get("newComplexInputPath"))
     val newMoveInputPath = scala.Option(hadoopConf.get("newMoveInputPath"))
     val newRepairInputPath = scala.Option(hadoopConf.get("newRepairInputPath"))
     val outputPath = scala.Option(hadoopConf.get("outputPath"))
     
     if (!newShipmentInputPath.isDefined || !newComplexInputPath.isDefined || !newMoveInputPath.isDefined || !newRepairInputPath.isDefined || !outputPath.isDefined) {
      System.err.println("usage: UnionAll " +
                    "-DnewShipmentInputPath=<arg> " +
                    "-DnewComplexInputPath=<arg> " +
                    "-DnewMoveInputPath=<arg>" +
                    "-DnewRepairInputPath=<arg>" +
                    "-DoutputPath=<arg>")
      System.exit(1)
     }
     
     val hdfs = FileSystem.get(hadoopConf)
     if (hdfs.exists(new Path(outputPath.get))) 
      hdfs.delete(new Path(outputPath.get), true)
      
     val sparkConf = new SparkConf().setAppName("Union Newshipment, Newcomplex, Newmove, Newrepair Table")
     val sc = new SparkContext(sparkConf)
     
     val newShipmentLines = sc.textFile(newShipmentInputPath.get)
     val newComplexLines = sc.textFile(newComplexInputPath.get)
     val newMoveLines = sc.textFile(newMoveInputPath.get)
     val newRepairLines = sc.textFile(newRepairInputPath.get)
     
     val unionAll = newShipmentLines.union(newComplexLines).union(newMoveLines).union(newRepairLines)
     
     unionAll.coalesce(820, true).saveAsTextFile(outputPath.get)
  }
}