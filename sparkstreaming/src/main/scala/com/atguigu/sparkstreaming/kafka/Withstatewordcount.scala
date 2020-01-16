package com.atguigu.sparkstreaming.kafka

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author gogo
 * @create 2020-01-15 15:25
 */
object Withstatewordcount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("withstatewordcount")
        val context: StreamingContext = new StreamingContext(conf,Seconds(2))

        context.checkpoint("./ck2") // 保存状态

        val sourceStreaming: ReceiverInputDStream[String] = context.socketTextStream("hadoop105",9999)
        val result1: DStream[(String, Int)] = sourceStreaming.flatMap(_.split("\\W+"))
            .map((_,1)).updateStateByKey{
            ((Seq:Seq[Int],opt:Option[Int]) =>
                Some(opt.getOrElse(0) + Seq.sum))
            }

        result1.print(100)
        context.start()
        context.awaitTermination()
    }
}
