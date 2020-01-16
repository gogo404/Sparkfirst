package com.atguigu.spark01.day05.DF_DS_RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author gogo
 * @create 2020-01-14 18:13
 */
// TODO: DF    .as[Object]  指定Object的类型 => Df
object DFtoDS {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("dftods")
            .getOrCreate()

        // 得到DataFrame
        val rdd1: RDD[gg] = ss.sparkContext.parallelize(List(new gg("zhangsan",1),new gg("zhangsi",2)))
        import ss.implicits._   // 用到隐式转换
        val df1: DataFrame = rdd1.toDF()
        // TODO: 最常用的 DF[Object]  => DS[Object] 
                            // todo as[]  泛型中的类型可能需要自己指定下
        val ds1: Dataset[gg] = df1.as[gg]
        ds1.show()
        ss.close()
    }

}
