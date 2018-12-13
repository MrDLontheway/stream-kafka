package com.wxstc.dl.stream

;

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import com.alibaba.fastjson.JSON
import com.wxstc.dl.bean._
import com.wxstc.dl.redis.JedisSingle
import com.wxstc.dl.sparkj.UserInfo
import com.wxstc.dl.util.{IKUtils, JsonUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

object Learning {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    //    LoggerLevels.setStreamingLogLevels(Level.WARN)
    val Array(brokers, topics, groupId, checkpoint) = args
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: DyDanMu <brokers> <topics> <groupid> <checkpoint>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <groupid> is a consume group
           |  <checkpoint> is spark streaming checkpointdir
           |
                    """.stripMargin)
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("dy_Count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(20))
    //TODO
    ssc.checkpoint(checkpoint)



    val spark = SparkSession.builder().config(conf)
      .config("es.nodes", "10.1.1.26,10.1.1.33,10.1.1.40,10.1.1.47")
      //HTTP默认端口为9200
      .config("es.port", "9200")
      .config("es.index.auto.create", "true")
      .config("pushdown", "true")
      .config("es.nodes.wan.only", "true")
      .config("es.mapping.date.rich", "false").getOrCreate()
    val stringRDD = spark.sparkContext.textFile("D:\\_zhauy\\spark-testdata\\input\\user", 3)
    val rd1 = spark.sparkContext.parallelize(Array(new UserInfo(),new UserInfo(),new UserInfo()),3)
    import spark.implicits._
    import spark.sql
    import org.elasticsearch.spark._
    val us = stringRDD.map(x=>{
      new UserInfo()
    })
    //sqlContext.esDF("spark/people","?q=wang" )

//    val r = Option[Tuple2[String,String]]()


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers, //"115.159.93.95:9092,115.159.78.168:9092,115.159.222.161:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId, //"groupA",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicMap = topics.split(",")
//    var offset:java.util.Map[TopicPartition,Long] = new util.HashMap[TopicPartition,Long]()
//    offset.put(new TopicPartition("dy_gift",0),0)
//    offset.put(new TopicPartition("dy_danmu",2),0)
//    offset.put(new TopicPartition("dy_gift",1),0)
//    offset.put(new TopicPartition("dy_gift",2),0)
//    offset.put(new TopicPartition("dy_danmu",0),0)
//    offset.put(new TopicPartition("dy_danmu",1),0)

    var offset:Map[TopicPartition,Long] = Map[TopicPartition,Long]()
    offset += (new TopicPartition("dy_gift",0)->1)
    offset+=(new TopicPartition("dy_danmu",2)->1)
    offset+=(new TopicPartition("dy_gift",1)->1)
    offset+=(new TopicPartition("dy_gift",2)->1)
    offset+=(new TopicPartition("dy_danmu",0)->1)
    offset+=(new TopicPartition("dy_danmu",1)->1)
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicMap, kafkaParams,offset)
    )

//    var offset:Map[TopicPartition,Long] = Map()
//    offset += ("1"->0)
//    val messages2 = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      kafkaParams,
//      Subscribe[String, String](topicMap, kafkaParams,)
//    )
    //弹幕dstream
    val dataKafka = messages.filter(x => {
      x.topic().equals("dy_danmu")
    })

    val data = dataKafka.map(x => {
      (x.key(), x.value())
    })
    //礼物dstream
    val giftKafka = messages.filter(x => {
      x.topic().equals("dy_gift")
    })
    val gift = giftKafka.map(x => {
      (x.key(), x.value())
    })

    try {
      data.print(10)
      messages.foreachRDD(rdd => {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.map(x=>{
          //offset.put(x.topicPartition(),x.untilOffset)
        })
//        println(JSON.toJSON(offset))
        rdd.foreachPartition(iter => {
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          val topar = o.topicPartition()
          println(s"OffsetRange :${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        })
        messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })
      //      dealWithGift(gift, ssc, sc)
      //      giftKafka.foreachRDD(rdd => {
      //        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //        rdd.foreachPartition(iter => {
      //          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      //          println(s"OffsetRange :${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      //        })
      //        giftKafka.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      //      })
    } catch {
      case e: Exception => {
        System.err.println("Exception is:" + e)
        throw new RuntimeException("Exception error!!!" + e.getMessage)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
