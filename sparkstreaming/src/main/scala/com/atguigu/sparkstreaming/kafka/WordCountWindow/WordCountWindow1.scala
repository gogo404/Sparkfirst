package com.atguigu.sparkstreaming.kafka.WordCountWindow

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author gogo
 * @create 2020-01-15 15:09
 */
// TODO: 企业中SparkStreaming对窗口函数的运用
object WordCountWindow1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("wordcountwindows1")
            .setMaster("local[2]")
        val context: StreamingContext = new StreamingContext(conf,Seconds(2))
// todo 用来保存状态
        context.checkpoint("./ck2")
        // todo 直接对读取数据源 的流 (DS) 进行开窗。企业中一般这么做，缺点：无法进行优化(窗口间可能会造成重复计算);优点： 代码简洁，简单。
        val sourceStream: DStream[String] = context.socketTextStream("hadoop105",9999).window(Seconds(12),Seconds(6))
        val ds1: DStream[(String, Int)] = sourceStream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_)
        ds1.print(100)

        context.start()
        context.awaitTermination()
    }
}
