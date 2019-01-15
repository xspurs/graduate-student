package com.brctl.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Score Statistics Spark Job
  * @author duanxiaoxing
  * @created 2019/1/12
  */
object ScoreStatistics {

  def main(args: Array[String]): Unit = {
    // init spark config
    val config = new SparkConf()
      .setAppName("Spark Score Statistics")
      .setMaster("local")

    // init spark sql
    val sparkSQL = SparkSession
      .builder()
      .config(config)
      .getOrCreate()

    // local csv file
    val filePath = "file:///Users/duanxiaoxing/Github/graduate-student/" +
      "big-data-assignment/input/student_score.csv"
    // data frame from local csv file
    val df = sparkSQL
      .read
      // infer schema by value
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(filePath)
    // original data frame
    df.show

    import org.apache.spark.sql.functions._

    /* write to file
    val outputDir = "file:///Users/duanxiaoxing/Github/graduate-student/" +
      "big-data-assignment/output"
    // order by score desc
    df.orderBy(desc("score")).write.format("parquet").mode(SaveMode.Append).csv(s"$outputDir/order_by_score")
    // mean by score
    df.groupBy().mean("score").write.format("parquet").mode(SaveMode.Overwrite).csv(s"$outputDir/mean_by_score")
    */

    // not write to file
    // order by score desc
    df.orderBy(desc("score")).show
    // mean by score
    df.groupBy().mean("score").show

    // stop spark sql
    sparkSQL.stop
  }

}
