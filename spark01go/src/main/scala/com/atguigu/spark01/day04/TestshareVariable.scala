package com.atguigu.spark01.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-10 8:13
 */
object TestshareVariable {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("testshareVariable")
        val context: SparkContext = new SparkContext(conf)
        // TODO: 1
//        val rdd: RDD[Int] = context.parallelize(List(1,2,3,4))=
//        val rdd2: RDD[Int] = rdd.map(_*2)
//        rdd2.foreach(println)   // 2\n 6\n 4\n 8
        // TODO: 2
        val p1: person = new person(10)
        val rdd1: RDD[person] = context.parallelize(Array(p1))
        rdd1.map{
            p =>
                println("-------------------")
                p.age = 1  // case class 样例类 (主构造中参数即为属性)默认属性为val类型。
                p
        }
        rdd1.collect()
        println(p1.age)

    }
}
case class person(var age:Int){

}
object test2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("testshareVariable")
        val context: SparkContext = new SparkContext(conf)
// TODO: 1
        val rdd: RDD[Int] = context.parallelize(List(1,2,3,4))
//        val rdd2: RDD[Int] = rdd.map(
//            _*2
//        )
//        rdd2.foreach(println)   // 2\n 6\n 4\n 8
        // TODO: 2
        var a :Int = 1
        rdd.map{
            x=>
            a += 1
            x
        }
        println(a)
    }
}
object test3 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("testshareVariable")
        val context: SparkContext = new SparkContext(conf)

        val rdd: RDD[Int] = context.parallelize(List(1,2,3,4))
        var a =1
        rdd.map{
            x=>
                a += 1
                x
        }
        println(a)
    }
}
