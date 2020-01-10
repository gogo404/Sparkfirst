package com.atguigu.spark01.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author shkstart
 * @create 2020-01-08 8:30
 */
object Testaction {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("method01")
        val context: SparkContext = new SparkContext(conf)

        val rdd1: RDD[(String, Int)] = context.parallelize(List(("a",2),("b",4),("c",6),("a",3),("b",2),("c",12),("a",22)))
        val rdd11: RDD[(String, Int)] = context.parallelize(List(("a",22),("b",0),("c",6),("a",22)))
//         val l: Long = rdd1.count()
//        println(l)
        // reduce
            // zeroValue 参与计算次数分区数加一
//        aggregate
        //fold
        // count
        // countByKey
        //

        // TODO foreach
        // rdd的数据写入到外部存储比如: mysql
        // 1. rdd1.collect() 先把数据拉倒驱动端, 再从驱动端向mysql
        // 2. 分区内的数据, 直接向mysql写
        /* rdd1.foreach(x => {
             // 建立到mysql的连接
             // 写
         })*/
//        rdd1.foreachPartition(it => {
            // 建立到mysql的连接
            // 写
//        })
//        rdd1.partitionBy()







    }

}
