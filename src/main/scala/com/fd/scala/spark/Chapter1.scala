package com.fd.scala.spark

import org.apache.spark.{SparkConf, SparkContext}

object Chapter1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("RDD_MOVIE_Users_Analyer")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")
    val dataPath = "file:///Users/fandong/Desktop/hadoop/SparkStandlone/src/data/moviedata/medium"
    val moviesRDD = sc.textFile(dataPath + "/movies.dat")
    val count = moviesRDD.map(_.split("::")).count()
    println(count)
    sc.stop()
  }
}
