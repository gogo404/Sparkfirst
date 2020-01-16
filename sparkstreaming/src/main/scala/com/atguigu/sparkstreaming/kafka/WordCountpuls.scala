package com.atguigu.sparkstreaming.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author gogo
 * @create 2020-01-15 9:27
 */
object WordCountpuls {
    def createssc () ={
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("wordcountplus")

        val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
        ssc.checkpoint("./ck1")
        val params: Map[String, String] = Map(ConsumerConfig.GROUP_ID_CONFIG -> "0830",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop105:9092,hadoop106:9092,hadoop107:9092"
        )
        val sourceStream= KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            Set("s0830")
        )
        sourceStream.map{case (_,v)=> v}.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_).print(1000)
       ssc
    }
    def main(args: Array[String]): Unit = {
       val  context = StreamingContext.getActiveOrCreate("./ck1",createssc)

        context.start()
        context.awaitTermination()

    }
}
