package com.cathay.dtag.spark.intro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by roger19890107 on 7/13/16.
  */
object RunWordCount {
  def main(args: Array[String]) {
    /**
      * Log setting
      */
    Logger.getLogger("org").setLevel(Level.OFF)
    //System.setProperty("spark.ui.showConsoleProgress", "false")

    /**
      * Setup spark context by configuration
      */
    println("Start to run WordCount...")
    val sc = new SparkContext(new SparkConf()
      .setAppName("wordCount")
      .set("spark.driver.memory", "2g")
      .setMaster("local[*]"))
    sc.setCheckpointDir("data/checkpoints")

    /**
      * Read text file
      */
    println("Start to read text file...")
    val textFile = sc.textFile("data/dataset/Leonardo.txt")

    /**
      * Count every words
      */
    println("Start to build RDD...")
    val countRDD = textFile.flatMap(line => line.split(" "))
        .map(x => (x, 1)).reduceByKey((y, z) => y + z)
      //.map((_, 1)).reduceByKey(_ + _)

    /**
      * Dump file
      */
    countRDD.saveAsTextFile("data/output/wordcount")
    println("save file successfully.")
  }
}
