package com.atguigu.spark01.day05.DF_DS_RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author gogo
 * @create 2020-01-14 18:07
 */
object RDDtoDS {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("rddtods")
            .getOrCreate
        import  ss.implicits._
        // RDD 中存放元组  RDD[()]
//        val rdd1: RDD[(String, Int)] = ss.sparkContext.parallelize(List(("zhangsan",1),("zhangsi",2),("zhangwu",3)))
//        val ds1: Dataset[(String, Int)] = rdd1.toDS()
//        ds1.show()
        val rdd1: RDD[gg] = ss.sparkContext.parallelize(List(new gg("zhangsan",1),new gg("zhangsi",2)))
        val ds2: Dataset[gg] = rdd1.toDS()
//        ds2.show()
        ds2.collect().foreach(println)
        ss.close()
    }
}
