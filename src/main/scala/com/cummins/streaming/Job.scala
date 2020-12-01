package com.cummins.streaming

import java.util.Properties

import com.cummins.library.ConnectPool
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Created by newforesee on 2020/11/30
 */
object Job {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val streaming: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(2))
    val frame: DataFrame = spark.read.jdbc(ConnectPool.getConf("url"), "table", ConnectPool.getProperties)
    val schema: StructType = frame.schema
    //streaming.checkpoint("hdfs://localhost:9000/ckp")
    val dstream: InputDStream[Row] = streaming.queueStream(mutable.Queue(frame.rdd), true)
    dstream.foreachRDD((x: RDD[Row]) =>{
      val df: DataFrame = spark.createDataFrame(x, schema)
      df.where($"age" > 18).show(100,false)
    })

    streaming.start()
    streaming.awaitTermination()



  }

}
