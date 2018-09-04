package com.wxstc.dl.stream

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.ProcessingTime
object SparkStructuredStreaming {

  def main(args: Array[String]): Unit = {
    val Array(brokers, topics, groupId, zkQuorum) = args

    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]"))//.enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("group.id",groupId)
      .option("startingoffset", "earliest")//"smallest")
      .option("subscribe", topics)
      .load()

//    df.createOrReplaceTempView("kafkaTable")
//    val kafkaQuery = spark.sql(
//      """
//        |select * from kafkaTable
//      """.stripMargin)
//      .writeStream
//      .format("console")
//      .trigger(ProcessingTime(2000L))
//      .start()
//    kafkaQuery.awaitTermination()

    val kafka = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "CAST(partition AS STRING)", "CAST(offset AS STRING)")
      .writeStream
      .format("console")
      .trigger(ProcessingTime(2000L))
      .start()

    kafka.awaitTermination()
  }
}
