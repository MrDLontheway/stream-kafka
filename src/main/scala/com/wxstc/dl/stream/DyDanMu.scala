package com.wxstc.dl.stream

import java.util

import com.alibaba.fastjson.JSON
import com.wxstc.dl.bean._
import com.wxstc.dl.redis.JedisSingle
import com.wxstc.dl.util.{IKUtils, JsonUtils}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object DyDanMu {
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }

  def dealWithDanMu(data: DStream[(String, String)],ssc: StreamingContext) = {

    val offsetRanges = data.asInstanceOf[HasOffsetRanges].offsetRanges
    //将json数据转成bean 然后进行数据过滤  ik分词 计数
    val words = data.map(x => {
      JsonUtils.jsonToPojo(x._2, classOf[Danmaku])
    })
      .filter(!_.isEmpty)
      .map(_.get.getContent).flatMap(IKUtils.ikAny(_)).filter(_.length > 2).map((_, 1))

    val wordCounts_shi = words.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60 * 1), Seconds(5)).transform(
      rdd => {
        val rddn = rdd.sortBy(_._2, false)
        if (rddn.take(10).size > 0) {
          val jedis = JedisSingle.jedisPool.getResource
          jedis.set("dy_danmu_wordsWindow", JsonUtils.objectToJson(rddn.take(10)))
          jedis.close()
        }
        rddn
      }
    )
    //与以往数据迭代聚合
    val wordCounts = words.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    //对数据结果排序
    val sortResult = wordCounts.transform(rdd => {
      val rddn = rdd.sortBy(_._2, false)
      if (rddn.take(10).size > 0) {
        val jedis = JedisSingle.jedisPool.getResource
        jedis.set("dy_danmu_words", JsonUtils.objectToJson(rddn.take(10)))
        jedis.close()
      }
      rddn
    })
    //sortResult.print()

    //对添加的直播间活跃度进行弹幕总计数 排序
    val live_danmuCountshi = data.map(x => {
      (x._1, 1)
    }).reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60 * 1), Seconds(5)).transform(
      rdd => {
        val rddn = rdd.sortBy(_._2, false)
        if (rddn.take(10).size > 0) {
          val jedis = JedisSingle.jedisPool.getResource
          jedis.set("dy_danmu_roomWindow", JsonUtils.objectToJson(rddn.take(10)))
          jedis.close()
        }
        rddn
      }
    )
    val live_danmuCount = data.map(x => {
      (x._1, 1)
    }).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true).transform(rdd => {
      val rddn = rdd.sortBy(_._2, false)
      val result = rddn.take(10).toMap
      if (result.size > 0) {
        val jedis = JedisSingle.jedisPool.getResource
        jedis.set("dy_danmu_room", JsonUtils.objectToJson(rddn.take(10)))
        jedis.close()
      }
      rddn
    })

    val user_liveshi = data.map(x => {
      JsonUtils.jsonToPojo(x._2, classOf[Danmaku])
    })
      .filter(!_.isEmpty)
      .map(x => {
        (x.get.getSnick, 1)
      }).reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60 * 1), Seconds(5)).transform(
      rdd => {
        val rddn = rdd.sortBy(_._2, false)
        if (rddn.take(10).size > 0) {
          val jedis = JedisSingle.jedisPool.getResource
          jedis.set("dy_danmu_liveUserWindow", JsonUtils.objectToJson(rddn.take(10)))
          jedis.close()
        }
        rddn
      }
    )
    //对用户活跃度 （弹幕总数）排序
    val user_live = data.map(x => {
      JsonUtils.jsonToPojo(x._2, classOf[Danmaku])
    })
      .filter(!_.isEmpty)
      .map(x => {
        (x.get.getSnick, 1)
      }).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true).transform(rdd => {
      val rddn = rdd.sortBy(_._2, false)
      val result = rddn.take(10).toMap
      if (result.size > 0) {
        val jedis = JedisSingle.jedisPool.getResource
        jedis.set("dy_danmu_liveUser", JsonUtils.objectToJson(rddn.take(10)))
        jedis.close()
      }
      rddn
    })

    data.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    sortResult.print(1)
    live_danmuCount.print(1)
    user_live.print(1)
    wordCounts_shi.print(1)
    user_liveshi.print(1)
    live_danmuCountshi.print(1)
  }

  def dealWithGift(gift: DStream[(String, String)], ssc: StreamingContext, sc: SparkContext) = {
    val offsetRanges = gift.asInstanceOf[HasOffsetRanges].offsetRanges
    //获取斗鱼礼物id 与名称对照表
    val jedis = JedisSingle.jedisPool.getResource
    val json = jedis.get("dyGiftInFo")
    val res2 = JSON.parseObject(json,classOf[util.HashMap[String, String]])

    val giftinfo = sc.broadcast(res2)
    //将json数据转成bean
    val gift1 = gift.map(x => {
      JsonUtils.jsonToPojo(x._2, classOf[DyGift])
    })
      .filter(!_.isEmpty)
      .map(mapFunc = x => {
        val giftids = giftinfo.value
        val row = x.get
        row.giftName = giftids.getOrDefault(row.gid,"未知")
        row
      })
    giftinfo.unpersist()//释放资源
    //计算直播间各个礼物总数
    val gfit2 = gift1.map(x=>{
      (x.rid+","+x.giftName,1)
    }).updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism), true).foreachRDD(rdd=>{
      val t = rdd.map(x=>{(x._1.split(",")(0),(x._1.split(",")(1)),x._2)}).groupBy(_._1).map(x=>{
        val dg = new DyRoomGift()
        dg.rid = x._1
        val r = x._2.toArray.map(x=>{
          new GiftSum(x._2,x._3)
        })
        dg.setGifts(r)
        dg
        //(x._1,x._2.toList.map(y=>{(y._2,y._3)}))
      }).collect()
//      val rddn = rdd.sortBy(_._2, false)
//      val result = rddn.take(10).toMap
      if (t.size > 0) {
        val jedis = JedisSingle.jedisPool.getResource
        jedis.set("dy_giftByRoom", JsonUtils.objectToJson(t))
        jedis.close()
      }
      rdd
    })

    //计算用户送出各个礼物总数
    val gfitUser = gift1.map(x=>{
      (x.nickName+","+x.giftName,1)
    }).updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism), true).foreachRDD(rdd=>{
      val t = rdd.map(x=>{(x._1.split(",")(0),(x._1.split(",")(1)),x._2)}).groupBy(_._1).map(x=>{
        val dg = new DyRoomGift()
        dg.rid = x._1
        val r = x._2.toArray.map(x=>{
          new GiftSum(x._2,x._3)
        })
        dg.setGifts(r)
        dg
        //(x._1,x._2.toList.map(y=>{(y._2,y._3)}))
      }).collect()
      //      val rddn = rdd.sortBy(_._2, false)
      //      val result = rddn.take(10).toMap
      if (t.size > 0) {
        val jedis = JedisSingle.jedisPool.getResource
        jedis.set("dy_giftByUser", JsonUtils.objectToJson(t))
        jedis.close()
      }
      rdd
    })
    gift.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }


  def main(args: Array[String]): Unit = {
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
    val ssc = new StreamingContext(sc, Seconds(5))
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

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicMap, kafkaParams)
    )
    //弹幕dstream
    val data = messages.filter(x=>{x.topic().equals("dy_danmu")})
      .map(x=>{
        (x.key(),x.value())
    })
    //礼物dstream
    val gift = messages.filter(x=>{x.topic().equals("dy_gift")})
      .map(x=>{
        (x.key(),x.value())
      })

    dealWithDanMu(data,ssc)
    dealWithGift(gift,ssc,sc)
    //val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    //data.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
