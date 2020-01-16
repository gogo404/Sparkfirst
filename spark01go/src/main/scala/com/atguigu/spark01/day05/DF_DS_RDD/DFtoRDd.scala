package com.atguigu.spark01.day05.DF_DS_RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * @author gogo
 * @create 2020-01-14 17:38
 */
// TODO: DataFrame .rdd  => RDD
object DFtoRDd {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("dftordd")
            .getOrCreate()
        import ss.implicits._
        val df1: DataFrame = ss.read.json("F:\\BigData\\Spark/jsondoc1.txt")
        val rdd1: RDD[Row] = df1.rdd
//        rdd1.collect().foreach(println)
        // row 存储极其难搞，不方便。
        val rdd2: RDD[(String, Int)] = rdd1.map {
                    //todo Row 中读取数据的方式
            row => (row.getString(2),
                row.getLong(0).toInt)
        }
        rdd2.collect().foreach(println)

        ss.close()
    }
}
