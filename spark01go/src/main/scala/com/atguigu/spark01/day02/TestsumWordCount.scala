package com.atguigu.spark01.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-07 19:20
 */
object TestsumWordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("sumwordcount")
        val context:SparkContext = new SparkContext(conf)
        val rdd: RDD[(String, Int)] = context.makeRDD(List(("a",1),("b",2),("c",3),("a",1)))
//        val rdd2: RDD[(String, Int)] = rdd.groupByKey().mapValues(_.toList.sum) // wordcount todo 2 groupByKey 和mapValues
//        rdd.groupBy(_._1).map{  // todo 2.2  groupBy 和 map来实现
//            case (string,it)=>
//                val intertem: Iterable[Int] = it.map(_._2)
//                (string,intertem.sum)
//        }.foreach(println)

//        rdd.reduceByKey(_+_).foreach(println) // wordcount todo 1

        // wordcount todo 3   aggregateByKey(zeroValue:U)(seqop:(U,v) => U,combOp:(U,U) => )  => RDD[(k,U)]
        //                              三个参数： 零值，分区内聚合op，分区间聚合op
        //rdd.aggregateByKey(0)(_+_,_+_).foreach(println)
        // flodByKey()()  两个参数： 零值，聚合操作op  aggregateByKey()()的简化版   //wordcount todo 4
//        rdd.foldByKey(0)(_+_).foreach(println)
            // todo  groupBy   和 groupByKey() 得到的型式是一样的。
//        val StringAndIterable: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
//        val StringAndIterable2: RDD[(String, Iterable[Int])] = rdd.groupByKey()











    }

}
