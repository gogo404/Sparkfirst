package com.atguigu.spark01.day05.DF_DS_RDD





import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author gogo
 * @create 2020-01-14 16:35
 */
// TODO: 1 RDD 中存储的是元组 RDD[()] .toDF("c1", "c2")   =>   DataFrame
object RDDtoDF1 {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("rddtodf")
            .getOrCreate()
        import ss.implicits._
        // 类型自己指定，自动推断可能会出现错误。(其实是我写的有问题，类型混合，不全。哈哈哈哈哈哈哈啊哈哈哈哈哈哈哈哈哈)
        val list:List[(String,Int)] = List(("zhangsan",1),("zhangsi",2),("zhangwu",3),("zhangliu",4))
        //得到 RDD
       val rdd1:RDD[(String,Int)] = ss.sparkContext.parallelize( list)
        // TODO 得到 RDD   .toDF(指定列名(字符串表示))    DataFrame
        val df1 :DataFrame = rdd1.toDF("name","age")
        df1.show()

        ss.close() //随手关闭Session
    }
}
// (最常用,更简单)TODO: 2 RDD 中存储的是样例类 RDD[Object] .toDF   =>   DataFrame
object RDDtoDF2 {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("rddtodf")
            .getOrCreate()
        import ss.implicits._

        //得到 RDD
         val rdd2: RDD[person] = ss.sparkContext.parallelize( List(new person("zhangsan",3),new person("zhangsi",4)))
        // TODO 得到 RDD   .toDF    DataFrame
        val df2 :DataFrame = rdd2.toDF()
        df2.show()

        ss.close() //随手关闭Session
    }
}
case class person(name:String,age:Int)
// (最常用,更简单)TODO: 2.2 装换使用SparkSession原生API  RDD 中存储的是样例类 RDD[Object] .toDF   =>   DataFrame
object RDDtoDF2_2 {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("rddtodf")
            .getOrCreate()
        import ss.implicits._

        //得到 RDD
        val rdd2_2: RDD[(String,Int)] = ss.sparkContext.parallelize(List(("zhangsan", 3),("zhangsi", 4)))
        // TODO 得到 RDD   .toDF    DataFrame
        // DataFrame里面存放的是Row,想通过格式转换，得到
        val rdd2_21: RDD[Row] = rdd2_2.map {
            case (a, b) => Row(a, b)
        }
        rdd2_21
        val schema:StructType = StructType(List(StructField("name",StringType),StructField("age",IntegerType)))
        // 上面这种写法可以简化为：
//        StructType(StructField("name",StringType)::StructField("age",IntegerType)::Nil)
        val df2_2: DataFrame = ss.createDataFrame(rdd2_21, schema)
         // 稍显复杂，需要用StructType 准确的标记 格式
        df2_2.show()

        ss.close() //随手关闭Session
    }
}