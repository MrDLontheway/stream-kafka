package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010

/**
  * KafkaHelper类提供两个共有方法，一个用来创建direct方式的DStream，另一个用来更新zookeeper中的消费偏移量
 *
  * @param kafkaPrams kafka配置参数
  * @param zkQuorum zookeeper列表
  * @param group 消费组
  * @param topic 消费主题
  */
class KafkaHelper(kafkaPrams:Map[String,String], zkQuorum:String, group:String, topic:String) extends Serializable{

  private val kc = new KafkaCluster(kafkaPrams)
  private val zkClient = new ZkClient(zkQuorum)
  private val topics = Set(topic)

  /**
    * 获取消费组group下的主题topic在zookeeper中的保存路径
    * @return
    */
  private def getZkPath():String={
    val topicDirs = new ZKGroupTopicDirs(group,topic)
    val zkPath = topicDirs.consumerOffsetDir
    zkPath
  }

  /**
    * 获取偏移量信息
    * @param children 分区数
    * @param zkPath zookeeper中的topic信息的路径
    * @param earlistLeaderOffsets broker中的实际最小偏移量
    * @param latestLeaderOffsets broker中的实际最大偏移量
    * @return
    */
  private def getOffsets(children:Int,zkPath:String,earlistLeaderOffsets:Map[TopicAndPartition, KafkaCluster.LeaderOffset],latestLeaderOffsets: Map[TopicAndPartition, KafkaCluster.LeaderOffset]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for(i <- 0 until children){
      //获取zookeeper记录的分区偏移量
      val zkOffset = zkClient.readData[String](s"${zkPath}/${i}").toLong
      val tp = TopicAndPartition(topic,i)
      //获取broker中实际的最小和最大偏移量
      val earlistOffset: Long = earlistLeaderOffsets(tp).offset
      val latestOffset: Long = latestLeaderOffsets(tp).offset
      //将实际的偏移量和zookeeper记录的偏移量进行对比，如果zookeeper中记录的偏移量在实际的偏移量范围内则使用zookeeper中的偏移量，
      //反之，使用实际的broker中的最小偏移量
      if(zkOffset>=earlistOffset && zkOffset<=latestOffset) {
        fromOffsets += (tp -> zkOffset)
      }else{
        fromOffsets += (tp -> earlistOffset)
      }
    }
    fromOffsets
  }

  /**
    * 创建DStream
    * @param ssc
    * @return
    */
  def createDirectStream(ssc:StreamingContext):InputDStream[(String, String)]={
    //----------------------获取broker中实际偏移量---------------------------------------------
    val partitionsE: Either[Err, Set[TopicAndPartition]] = kc.getPartitions(topics)
    if(partitionsE.isLeft)
      throw new SparkException("get kafka partitions failed:")
    val partitions = partitionsE.right.get
    val earlistLeaderOffsetsE: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kc.getEarliestLeaderOffsets(partitions)
    if(earlistLeaderOffsetsE.isLeft)
      throw new SparkException("get kafka earlistLeaderOffsets failed:")
    val earlistLeaderOffsets: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earlistLeaderOffsetsE.right.get
    val latestLeaderOffsetsE: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kc.getLatestLeaderOffsets(partitions)
    if(latestLeaderOffsetsE.isLeft)
      throw new SparkException("get kafka latestLeaderOffsets failed:")
    val latestLeaderOffsets: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = latestLeaderOffsetsE.right.get
    //----------------------创建kafkaStream----------------------------------------------------
    var kafkaStream:InputDStream[(String, String)]=null
    val zkPath: String = getZkPath()
    val children = zkClient.countChildren(zkPath)
    //根据zookeeper中是否有偏移量数据判断有没有消费过kafka中的数据
    if(children > 0){
      val fromOffsets:Map[TopicAndPartition, Long] = getOffsets(children,zkPath,earlistLeaderOffsets,latestLeaderOffsets)
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      //如果消费过，根据偏移量创建Stream
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaPrams, fromOffsets, messageHandler)
    }else{
      //如果没有消费过，根据kafkaPrams配置信息从最早的数据开始创建Stream
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPrams, topics)
    }
    kafkaStream
  }

  /**
    * 更新zookeeper中的偏移量
    * @param offsetRanges
    */
  def updateZkOffsets(offsetRanges:Array[OffsetRange])={
    val zkPath: String = getZkPath()
    for( o <- offsetRanges){
      val newZkPath = s"${zkPath}/${o.partition}"
      //将该 partition 的 offset 保存到 zookeeper
      ZkUtils.updatePersistentPath(zkClient, newZkPath, o.fromOffset.toString)
    }
  }

}
