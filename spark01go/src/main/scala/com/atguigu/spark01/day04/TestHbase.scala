package com.atguigu.spark01.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-10 8:59
 */
object TestHbase {
    // TODO: read
    //       write
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("testhbase").setMaster("local[2]")
        val context: SparkContext = new SparkContext(conf)

//        newAPIHadoopRDD












    }

}
