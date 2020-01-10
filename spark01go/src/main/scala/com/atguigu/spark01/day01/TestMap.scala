package com.atguigu.spark01.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-06 20:07
 */
object TestMap {
    def main(args: Array[String]): Unit = {
        // TODO MAP
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testmap")
        val context: SparkContext = new SparkContext(conf)

        val list1: List[Int] = List(1,2,3,4,5)
        val list2: List[String] = List("zhangsan","sanzhang","aaa","abc")
        val rdd1: RDD[Int] = context.parallelize(list1,2)
        val rdd2: RDD[String] = context.parallelize(list2)
//          todo map()   结构转换/ 映射
//        rdd1.map(_*2).collect().foreach(println)
          //todo mapPartitions()
//        rdd1.mapPartitions(x => x.map(_ * 2),true).collect().foreach(println)
        // todo mapPartitionWithIndex()   mapPartitons()操作，同时withIndex

//        rdd1.mapPartitionsWithIndex{
//            (index,x)=>x.map(y=>(index,y*2))
//        }.foreach(println)
// todo flatMap()
//        rdd2.flatMap(x=>x).foreach(println)  // 扁平化











    }

}
