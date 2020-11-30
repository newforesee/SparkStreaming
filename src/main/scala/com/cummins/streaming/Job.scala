package com.cummins.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by newforesee on 2020/11/30
 */
object Job {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val streaming = new StreamingContext(spark.sparkContext, Seconds(2))



  }

}
