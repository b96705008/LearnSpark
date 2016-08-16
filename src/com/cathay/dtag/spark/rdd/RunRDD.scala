package com.cathay.dtag.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by roger19890107 on 7/26/16.
  */
object RunRDD {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Start to run WordCount...")
    val sc = new SparkContext(new SparkConf()
      .setAppName("RunRDD")
      .set("spark.driver.memory", "2g")
      .setMaster("local[*]"))
    sc.setCheckpointDir("data/checkpoints")

    // create RDD from log.txt
    val inputRDD = sc.textFile("data/dataset/log.txt")

    // filter from input
    val errorsRDD = inputRDD.filter(line => line.contains("error"))
    val warningsRDD = inputRDD.filter(line => line.contains("warning"))

    // union
    val badLinesRDD = errorsRDD.union(warningsRDD)

    badLinesRDD.collect().foreach(println)

    val input = sc.parallelize(List(1, 2, 3, 3))
    val result = input.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val avg = result._1 / result._2.toDouble
    println(avg)

  }
}
