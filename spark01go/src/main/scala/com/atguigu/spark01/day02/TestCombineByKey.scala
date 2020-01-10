package com.atguigu.spark01.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author shkstart
 * @create 2020-01-07 11:26
 */
object TestCombineByKey {
    def main(args: Array[String]): Unit = {
        // TODO combineByKey
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testpartitions")
        val context: SparkContext = new SparkContext(conf)

        val list:List[(String,Int)] = List(("a",2),("b",4),("c",6),("a",3),("b",2),("c",12),("a",22))
        val unit: RDD[(String, Int)] = context.parallelize(list,3)
        // TODO  所学聚合算子底层全部是调用的combineByKeyWithClassTag() : reduceByKey() foldByKey() aggregateByKey() combineBykey()

        // groupByKey mapValues







    }

}
