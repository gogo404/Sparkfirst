package com.atguigu.sparkstreaming.sparkstreaming01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// StreamingContext  初识：
object WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("wordcount")
            .setMaster("local[2]")
        // todo 1 入口对象  StreamingContext
        val context: StreamingContext = new StreamingContext(conf,Seconds(3))
        // todo 2 从数据源得到 流DStream
        val dstream1: ReceiverInputDStream[String] = context.socketTextStream("hadoop105",9999)
        // todo 3 对流操作(处理数据)
        val wordcount: DStream[(String, Int)] = dstream1.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
        // todo 4 展示数据 (行动算子)
        wordcount.print
        // todo 5 启动流处理
        context.start()
        // todo 6 阻止main函数退出， // 阻塞方法，阻塞主线程。
        context.awaitTermination()

    }
}
