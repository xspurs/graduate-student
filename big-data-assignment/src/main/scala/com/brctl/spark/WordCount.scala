package com.brctl.spark


import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("Spark WordCount").setMaster("local")
    val sc = new SparkContext(config)

    // read local file
    val filePath = "file:///Users/duanxiaoxing/Github/graduate-student/" +
      "big-data-assignment/input/to_word_count.txt"
    val file = sc.textFile(filePath)
    // rdd transform & action
    file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)
  }

}
