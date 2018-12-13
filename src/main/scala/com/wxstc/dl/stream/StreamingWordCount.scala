package com.wxstc.dl.stream

//import com.wxstc.util.LoggerLevels
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamingWordCount {
  val updateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    iterator.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(n => (x, n)) }
  }

  //业务处理
  def myDealService(rdd: RDD[ConsumerRecord[String, String]], offsetRanges: Array[OffsetRange]) = {
    rdd.foreachPartition(iter => {
      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      println(s"OffsetRange :${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    })
    val rdd1 = rdd.map(record => {
      (record.key(), record.value())
    })
  }

  val myUpstate = (it: Iterator[(String, Seq[String], Option[String])]) =>{
    it.map(x=>{
      (x._1,x._3.getOrElse(""))
    })
  }

  def main(args: Array[String]): Unit = {
    //    LoggerLevels.setStreamingLogLevels(Level.WARN)
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("D:\\_vm\\ck")
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <groupid> is a consume group
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(brokers, topics, groupId, zkQuorum) = args
    val topicsSet = topics.split(",")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers, //"115.159.93.95:9092,115.159.78.168:9092,115.159.222.161:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId, //"groupA",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //直连方式接入kafka数据流
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    //拿到数据流内需要的数据 key value
    val ds2 = messages.map(rec=>{
      (rec.key(),rec.value())
    })

    val ds3 = ds2.updateStateByKey(myUpstate,new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    /**
      * 拿到数据流后对每个  微批次 也就是rdd进行业务流程操作 这里需要注意的偏移量的管理问题
      * 1.通过kafka自动管理偏移量  (不推荐 不高可用，容易导致数据重复消费)
      * 2.自己手动管理偏移量  (推荐，但是需要自己维护偏移量  可以保存zk，hdfs 多种途径)
      * 3.自己手动提交偏移量   (推荐，可以保证数据消费可靠性，以及开发的简易型 )
      * 这里我采用的是第三种  这里再次提醒一下消费者偏移量建议用kafka的方式 不要用老版zk的方式
      */
    messages.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      myDealService(rdd,offsetRanges)
      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    val ds1 = messages.map(record => (record.key, record.value))
    ds1.foreachRDD(rdd => {
      println(rdd.count())
    })

    //    ds1.print(5)
    ds1.print(5)
    ssc.start()
    ssc.awaitTermination()
  }
}
