package com.atguigu.realtime.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author gogo
 * @create 2020-01-15 16:30
 */
trait App {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("app")
            .setMaster("local[2]")
        val context: StreamingContext = new StreamingContext(conf,Seconds(5))


    }
}
