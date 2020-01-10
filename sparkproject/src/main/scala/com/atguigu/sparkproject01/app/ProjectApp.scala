package com.atguigu.sparkproject01.app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.tukaani.xz.SPARCOptions

/**
 * @author shkstart
 * @create 2020-01-10 14:14
 */
object ProjectApp {
    def main(args: Array[String]): Unit = {
        // 1读取数据
        val conf: SparkConf = new SparkConf()
            .setAppName("project01")
            .setMaster("local[2]")
        val context: SparkContext = new SparkContext(conf)

        val sourceRDD: RDD[String] = context.textFile("")  // 路径
        // 2













    }

}
