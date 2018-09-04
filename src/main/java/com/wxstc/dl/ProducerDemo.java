package com.wxstc.dl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerDemo implements Runnable{
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public ProducerDemo(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "AnserInsight01:9092,AnserInsight02:9092,AnserInsight03:9092");//连接kafka集群 broker 列表
//        props.put("bootstrap.servers", "192.168.0.16:9092");
        /**
         * 此配置是 Producer 在确认一个请求发送完成之前需要收到的反馈信息的数量。 这个参数是为了保证发送请求的可靠性。以下配置方式是允许的：
         acks=0 如果设置为0，则 producer 不会等待服务器的反馈。该消息会被立刻添加到 socket buffer 中并认为已经发送完成。在这种情况下，服务器是否收到请求是没法保证的，并且参数retries也不会生效（因为客户端无法获得失败信息）。每个记录返回的 offset 总是被设置为-1。
         acks=1 如果设置为1，leader节点会将记录写入本地日志，并且在所有 follower 节点反馈之前就先确认成功。在这种情况下，如果 leader 节点在接收记录之后，并且在 follower 节点复制数据完成之前产生错误，则这条记录会丢失。
         acks=all 如果设置为all，这就意味着 leader 节点会等待所有同步中的副本确认之后再确认这条记录是否发送完成。只要至少有一个同步副本存在，记录就不会丢失。这种方式是对请求传递的最有效保证。acks=-1与acks=all是等效的。
         all 与 -1 等效的
         */
        props.put("acks", "all");
        /**
         * 若设置大于0的值，则客户端会将发送失败的记录重新发送，尽管这些记录有可能是暂时性的错误。请注意，这种 retry 与客户端收到错误信息之后重新发送记录并无区别。允许 retries 并且没有设置max.in.flight.requests.per.connection 为1时，记录的顺序可能会被改变。比如：当两个批次都被发送到同一个 partition ，第一个批次发生错误并发生 retries 而第二个批次已经成功，则第二个批次的记录就会先于第一个批次出现。
         */
        props.put("retries", 0);
        /**
         * 当将多个记录被发送到同一个分区时， Producer 将尝试将记录组合到更少的请求中。这有助于提升客户端和服务器端的性能。这个配置控制一个批次的默认大小（以字节为单位）。
         当记录的大小超过了配置的字节数， Producer 将不再尝试往批次增加记录。

         发送到 broker 的请求会包含多个批次的数据，每个批次对应一个 partition 的可用数据

         小的 batch.size 将减少批处理，并且可能会降低吞吐量(如果 batch.size = 0的话将完全禁用批处理)。 很大的 batch.size 可能造成内存浪费，因为我们一般会在 batch.size 的基础上分配一部分缓存以应付额外的记录。
         */
        props.put("batch.size", 16384);
        /**
         * producer 会将两个请求发送时间间隔内到达的记录合并到一个单独的批处理请求中。通常只有当记录到达的速度超过了发送的速度时才会出现这种情况。然而，在某些场景下，即使处于可接受的负载下，客户端也希望能减少请求的数量。这个设置是通过添加少量的人为延迟来实现的&mdash；即，与其立即发送记录， producer 将等待给定的延迟时间，以便将在等待过程中到达的其他记录能合并到本批次的处理中。这可以认为是与 TCP 中的 Nagle 算法类似。这个设置为批处理的延迟提供了上限:一旦我们接受到记录超过了分区的 batch.size ，Producer 会忽略这个参数，立刻发送数据。但是如果累积的字节数少于 batch.size ，那么我们将在指定的时间内“逗留”(linger)，以等待更多的记录出现。这个设置默认为0(即没有延迟)。例如：如果设置linger.ms=5 ，则发送的请求会减少并降低部分负载，但同时会增加5毫秒的延迟。
         * 增加请求延迟 降低负载
         */
        props.put("linger.ms", 1);
        /**
         * Producer 用来缓冲等待被发送到服务器的记录的总字节数。如果记录发送的速度比发送到服务器的速度快， Producer 就会阻塞，如果阻塞的时间超过 max.block.ms 配置的时长，则会抛出一个异常。
         这个配置与 Producer 的可用总内存有一定的对应关系，但并不是完全等价的关系，因为 Producer 的可用内存并不是全部都用来缓存。一些额外的内存可能会用于压缩(如果启用了压缩)，以及维护正在运行的请求。
         */
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    public void run() {
        int messageNo = 1;
        try {
            for(;;) {
                String messageStr="你好，这是第"+messageNo+"条数据";
                System.out.println("成功发送了"+messageNo+"条");
                RecordMetadata metadata = producer.send(new ProducerRecord<String, String>(topic, "Message", messageStr)).get();
                // 程序阻塞，直到该条消息发送成功返回元数据信息或者报错
                StringBuilder sb = new StringBuilder();
                sb.append("record [").append(metadata.serializedKeySize()+":"+metadata.serializedValueSize()).append("] has been sent successfully!").append("\n")
                        .append("send to partition ").append(metadata.partition())
                        .append(", offset = ").append(metadata.offset());
                System.out.println(sb.toString());
                //生产了10条就打印
//                if(messageNo%100==0){
//                    //System.out.println("发送的信息:" + messageStr);
//                }
                //生产100条就退出
                if(messageNo%10==0){
                    System.out.println("成功发送了"+messageNo+"条");
                    break;
                }
                messageNo++;
				Utils.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String[] args) {
        ProducerDemo pd = new ProducerDemo("tstmessage");
        Thread t1 = new Thread(pd);
        t1.start();
    }
}
