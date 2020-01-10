package com.atguigu.spark01.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2
import org.spark_project.jetty.util.IteratingNestedCallback
import scala.math.Ordering.DoubleOrdering

/**
 * @author shkstart
 * @create 2020-01-10 10:41
 */
object TestAccumulatormultValue {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("testmymapacc")
        val context: SparkContext = new SparkContext(conf)
        val unit: RDD[Int] = context.parallelize(List(1,2,4,2,3,3,3,3,3))

        val mapacc: MyMapAcc = new MyMapAcc()
            context.register(mapacc,"mapacca")

//        var a:Int = 10
        unit.map{
            aa =>
                mapacc.add(1)
                aa
        }
        println(mapacc)

    }

}
// (sum,count,avg)   map中类型通过Key让别人更易连接，数字含义。
class MyMapAcc extends AccumulatorV2[Long,Map[String,Double]]{
    private var map:Map[String,Double] =Map()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[Long, Map[String, Double]] = {
        val acc: MyMapAcc = new MyMapAcc()
        acc.map = map
        acc
    }

    override def reset(): Unit =         // 返回新的最简单
        map = Map[String,Double]()

    override def add(v: Long): Unit ={
        // sum : 求和   count：计数
        map += "sum" -> map.getOrElse("sum",0D)
        map += "count" -> map.getOrElse("count",0D)
    }


    override def merge(other: AccumulatorV2[Long, Map[String, Double]]): Unit = other match {
        case o : MyMapAcc => this.map +=  "sum"-> (o.map.getOrElse("sum",0D) + this.map.getOrElse("sum",0D))
           this.map += "count" -> (o.map.getOrElse("count",0D) + this.map.getOrElse("count",0D))

        case _ => throw new UnsupportedOperationException
    }

    override def value: Map[String, Double] = {
        val avg: Double = map.getOrElse("sum",0D) / map.getOrElse("count",0D)
        map += "avg" -> avg
        map
    }
}