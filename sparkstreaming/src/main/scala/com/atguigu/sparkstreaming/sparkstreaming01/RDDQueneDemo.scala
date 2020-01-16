package com.atguigu.sparkstreaming.sparkstreaming01

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable


/**
 * @author gogo
 * @create 2020-01-13 15:37
 */
// TODO wordcount2 数据源是Queue队列(常用这种数据源做测试，和压测)
object RDDQueneDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("rddquenue")
        val context: StreamingContext = new StreamingContext(conf,Seconds(5))

        val context1: SparkContext = context.sparkContext

        val queue1:mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
        val rddds: InputDStream[Int] = context.queueStream(queue1,true)
        rddds.reduce(_+_).print

        context.start()

        for(elem <-1 to 3){
            queue1.enqueue(context1.parallelize(1 to 5))
            Thread.sleep(100)
        }
        context.awaitTermination()

    }

}
