package com.atguigu.sparkstreaming.kafka



import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
 * @author gogo
 * @create 2020-01-13 20:25
 */
//TODO  数据来源为kafka： 非常重要()的一种情况。
object WordCount_kafka {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("wordcountkafka")
            .setMaster("local[2]")
        val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
        // 消费者组，
        val params: Map[String, String] = Map[String, String](
            "group.id" -> "0830",
            "bootstrap.servers" -> "hadoop105:9092,hadoop106:9092,hadoop107:9092"
            // 注意导类，  从常量中寻找
//                ConsumerConfig.GROUP_ID_CONFIG
//            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
        )
        // createDirectStream直连(createStream 先读取到接收器，再由接收器分发到各executor，新版本已经取消 这种方式

                    // k (kafka中k一般为null，)     v(为传输中的有价值的数据)
        val sourceStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            Set("s0830") //  消费者 订阅的主题
        )
        // todo 此时消费数据 做到至少一次

//        sourceStream.print(1000)
        sourceStream.map{case (_,v)=> v}.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_).print(1000)
        ssc.start()
        ssc.awaitTermination()
    }
}

//TODO 启动Kafaka生产者。 主题名topic s0830
//./kafka-console-producer.sh --broker-list hadoop105:9092 --topic s0830
