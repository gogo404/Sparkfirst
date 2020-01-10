package com.atguigu.spark01.day03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author shkstart
 * @create 2020-01-08 10:16
 */
object TestMethod03 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("method01")
        val context: SparkContext = new SparkContext(conf)

        val rdd1: RDD[(String, Int)] = context.parallelize(List(("a",2),("b",4),("c",6),("a",3),("b",2),("c",12),("a",22)))
        val rdd11: RDD[(String, Int)] = context.parallelize(List(("a",22),("b",0),("c",6),("a",22)))
        // 持久化，记住有向图。
//        rdd1.persist()
//        rdd1.cache()

        // checkpoint：一定在磁盘

        // 都在第一次job执行完之后再执行。顺序没关系。
//        checkpoint()
//        cache()
//        rdd1.cogroup()







    }

}
