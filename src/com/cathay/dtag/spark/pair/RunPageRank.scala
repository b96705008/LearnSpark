package com.cathay.dtag.spark.pair

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by roger19890107 on 8/16/16.
  */
object RunPageRank extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  println("Start to run WordCount...")
  val sc = new SparkContext(new SparkConf()
    .setAppName("RunPR")
    .set("spark.driver.memory", "2g")
    .setMaster("local[*]"))
  sc.setCheckpointDir("data/checkpoints")

  // Init (page A, Seq(pages link to A))
  val links = sc.parallelize(List(
      ("a", Seq("b", "c")),
      ("b", Seq("c", "d", "e")),
      ("d", Seq("a", "c")),
      ("e", Seq("b"))
    ))
    .partitionBy(new HashPartitioner(5))
    .persist()

  // Initialize each page's rank to 1.0; since we use mapValues, the resulting RDD
  // will have the same partitioner as links
  var ranks = links.mapValues(v => 1.0)

  // Run 10 iterations of PageRank
  for (i <- 0 until 10) {
    val contributions = links.join(ranks).flatMap {
      case (pageId, (links, rank)) =>
        links.map(dest => (dest, rank / links.size))
    }
    ranks = contributions
      .reduceByKey(_ + _)
      .mapValues(v => 0.15 + 0.85 * v)
  }

  ranks.saveAsTextFile("data/output/pagerank")
}
