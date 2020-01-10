package com.atguigu.spark01.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-08 20:47
 */
object TestText {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("TestText")
        val context:SparkContext = new SparkContext(conf)

        val unit: RDD[Int] = context.parallelize(List(1,2,3,4))
//        unit.saveAsTextFile("./testtext")
        context.textFile("./testtext").countByValue().foreach(println)

        context.stop()

    }
}
