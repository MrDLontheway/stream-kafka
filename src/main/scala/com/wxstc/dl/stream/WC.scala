package com.wxstc.dl.stream;

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object WC {
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
    val ssc = new StreamingContext(sc, Seconds(20))
    //TODO
    ssc.checkpoint(checkpoint)

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
      Subscribe[String, String](topicMap, kafkaParams)
    )

    val ds1 = messages.map(x=>{
      (x.key(),x.value())
    })
    val d2 = ds1.map(x=>("1",x))

    var da: DStream[(String, (String, String))] = d2.updateStateByKey[Tuple2[String, String]]((values: Seq[Tuple2[String, String]], state: Option[Tuple2[String, String]]) => {
//      dftmstationlineinfo.createOrReplaceTempView("TM_STATION_LINE_INFO")
      println(state)
      var newValue = state.getOrElse(Tuple2[String, String]("", ""))
      var distance = newValue._1.toFloat
      var reachtime = newValue._2.toString
      for (value <- values) {
        //todo 求两个站点的距离
        distance = value._1.toFloat-distance
        //todo 求两个站点使用时间
        var df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        var thistime= df.parse(value._2).getTime
        var lasttime= df.parse(reachtime).getTime
        var datatime =(thistime-lasttime)/1000/60/60
        var  speed3=distance/1000/datatime

        //todo 核对数据
        println("本次站点id:"+reachtime)
        println("上次站点和总距离id:"+distance)
        println("时间间隔:"+datatime)
        println("运行速度:"+speed3)

        // reachtime=speed3.toString
        //reachtime=(thistime-lasttime).toString
        reachtime=reachtime
      }
      println("=========")
      Option[Tuple2[String, String]](distance.toString,reachtime)

      var now: Date = new Date()
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm")
      var date = dateFormat.format(now)
      /*if(date.equals("18:02")){
         Option[Tuple2[Int,Int]](0,0)
      }*/
      //模式匹配。指定某点自动清除数据
      date match {
        case "11:32" => Option[Tuple2[String, String]]("", "")
//        case _ => Option[Tuple2[String, String]](distance, reachtime)
      }

    })



    //弹幕dstream
    ds1.print()

    try {
      messages.foreachRDD(rdd => {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition(iter => {
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          val topar = o.topicPartition()
          println(s"OffsetRange :${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        })
        messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })
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
