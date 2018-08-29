package com.wxstc.dl.stream

//import com.wxstc.util.LoggerLevels
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamingWordCount {
  val updateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    iterator.flatMap{case(x,y,z)=> Some(y.sum + z.getOrElse(0)).map(n=>(x, n))}
  }

  def main(args: Array[String]): Unit = {
//    LoggerLevels.setStreamingLogLevels(Level.WARN)
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

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

    val Array(brokers, topics, groupId,zkQuorum) = args
    val topicsSet = topics.split(",")
    //    val kafkaParams = Map[String, String](
    //      "metadata.broker.list" -> brokers,
    //      "group.id" -> groupId,
    //      "auto.offset.reset" -> "smallest"
    //      //,"zookeeper.connect" -> ""
    //    )

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,//"115.159.93.95:9092,115.159.78.168:9092,115.159.222.161:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,//"groupA",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /*val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    val ds1 = messages.map(record => (record.key, record.value))*/
    //val km = new KafkaManager(kafkaParams)
    ssc.checkpoint("E:\\_vm\\ck")

    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    //socket 接收数据 127.0.0.1
    val zk = new ZkClient(zkQuorum)
    var fromOffsets: Map[TopicPartition, Long] = Map() //kafka 偏移量管理

    val stream = if (zk.znodeIsExists(s"${topic}offset")) {
      val nor = zk.znodeDataGet(s"${topic}offset")
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)//创建以topic，分区为k 偏移度为v的map
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      println(s"[ DealFlowBills2 ] topic ${nor(0).toString}")
      println(s"[ DealFlowBills2 ] Partition ${nor(1).toInt}")
      println(s"[ DealFlowBills2 ] offset ${nor(2).toLong}")
      println(s"[ DealFlowBills2 ] zk中取出来的kafka偏移量★★★ $newOffset")
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, newOffset))
    } else {
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      println(s"[ DealFlowBills2 ] 第一次计算,没有zk偏移量文件")
      println(s"[ DealFlowBills2 ] 手动创建一个偏移量文件 ${topic}offset 默认从0号分区 0偏移度开始计算")
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      zk.znodeCreate(s"${topic}offset", s"$topic,0,0")
      val nor = zk.znodeDataGet(s"${topic}offset")
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, newOffset))
    }

    //    val Array(zkQuorum, group, topics, numThreads) = args
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    ds1.foreachRDD(rdd=>{
      println(rdd.count())
    })
    ds1.print(5)
    ssc.start()
    ssc.awaitTermination()
  }
}
