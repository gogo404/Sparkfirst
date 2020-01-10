package com.atguigu.spark01.day01

import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-06 19:29
 */
object TestCreateRDD {
  def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("createrdd")

      val context: SparkContext = new SparkContext(conf)
      val list1: List[Int] = List(1,2,3,4,5)
      val list2: List[String] = List("zhangsan","sanzhang","aaa","abc")
      // TODO create RDD
      // todo 1 parallelize()方法，创建RDD ,可以指定分区数，默认为内核数  （每个分区数，运行一个任务）
      val rdd1: RDD[Int] = context.parallelize(list1,2)
      val rdd2: RDD[String] = context.parallelize(list2)
    // todo 2 makeRDD() 方法和parallelize()方法创建RDD是一样的。可以指定分区数
      val rdd11: RDD[Int] = context.makeRDD(list1,2)
    // todo 3 从外部存储创建RDD，.
        // todo 从其他RDD转换得到新的RDD
    
    
    
  }

}
