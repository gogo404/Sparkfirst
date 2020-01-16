package com.atguigu.spark01.day05.DF_DS_RDD

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author gogo
 * @create 2020-01-14 17:57
 */

object CreateDS {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("createds")
            .getOrCreate()
        import ss.implicits._
//  sequences  to  Dataset   非常方便 。
        val seq: Seq[gg] = Seq(new gg("zhangsan",1),new gg("zhangsi",2))
        val ds1: Dataset[gg] = seq.toDS()  // .toDS toDF  这种方法都用了隐式转换。不是SparkSession原生的方法。
        ds1.show()

        ss.close()


    }
}
case class gg(name:String,age:Int)