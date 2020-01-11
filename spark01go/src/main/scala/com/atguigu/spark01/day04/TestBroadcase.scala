package com.atguigu.spark01.day04

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-10 11:27
 */
object TestBroadcase {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("testbroadcase")
        val context: SparkContext = new SparkContext(conf)
        // TODO: 2个execute 4个分区
        val rdd: RDD[Int] = context.parallelize(List(1,25,2,4,5),4)

        val set: Set[Int] = Set(2,5)
        val bc: Broadcast[Set[Int]] = context.broadcast(set)
        val rdd2: RDD[Int] = rdd.filter(x=> bc.value.contains(x))
        rdd2.collect.foreach(println)

        context.stop()






    }
}
