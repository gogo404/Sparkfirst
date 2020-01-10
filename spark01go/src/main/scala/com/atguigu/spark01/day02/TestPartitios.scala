package com.atguigu.spark01.day02

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.glassfish.jersey.internal.util.collection.StringIgnoreCaseKeyComparator
import scala.reflect.ClassTag

/**
 * @author shkstart
 * @create 2020-01-07 9:15
 */
object TestPartitios {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testpartitions")
        val context: SparkContext = new SparkContext(conf)

        val list:List[(String,Int)] = List(("a",2),("b",4),("c",6),("a",3),("b",2),("c",12),("a",22))
        val unit: RDD[(String, Int)] = context.parallelize(list,3)
        val unit2: RDD[Int] = context.parallelize(List(2,4,8,6,10))
//        unit2.partitionBy() //todo error  存在隐式转换，RDD => PairRDDFunctions

//        val unit1: RDD[(Int, Int)] = unit2.map((_, 1)).partitionBy(new HashPartitioner(2))
//        println(unit1.glom().collect.mkString(","))  // todo HashPartitioner可能严重的数据倾斜，[Lscala.Tuple2;@3bc735b3,[Lscala.Tuple2;@577f9109

//        unit.partitionBy(new HashPartitioner(2)) .foreach(println)
//        unit.reduceByKey()

//        unit.groupBy(_._1).foreach(println)  // todo 一定有shuffle，没有预聚合，因为就没有聚合操作
//        (b,CompactBuffer((b,4)))
//        (a,CompactBuffer((a,2)))
//        (c,CompactBuffer((c,6)))
//        unit.groupByKey().foreach(println)
//        (b,CompactBuffer(4))
//        (a,CompactBuffer(2))
//        (c,CompactBuffer(6))
        // todo 预聚合    聚合函数
//        val rddnu1: RDD[(String, Int)] = unit.reduceByKey(_+_)
//        rddnu1.foreach(println)
            // 柯里化：理论基础闭包(函数参数在子函数中仍然可以使用。)
        //TODO def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]
//        unit.aggregateByKey(0)(_+_,_+_)

        // 求每个分区对应key的最大值，然后再把key相同的合并(value相加) zeroValue 迭代的初值，不要影响最终结果(所以即是最终结果的一个限(上限或者下限)。
//        val unit1: RDD[(String, Int)] = unit.aggregateByKey(Int.MinValue)((x,y) => x.max(y),(x,y) => x+y)
        // 最大值 最小值
//        val unit1: RDD[(String, (Int, Int))] = unit.aggregateByKey((Int.MinValue, Int.MaxValue))(
//            {
//                case ((x, y), z) => (x.max(z), y.min(z))
//            },
//            {
//                case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)
//            })
//        unit1.foreach(println)//(c,(18,18))(a,(27,27))(b,(6,6))

            //todo 求平均值

        // TODO foldByKey()     aggregateByKey() 的简化版，sepop 和 combop 一样，零值的类型和返回值类型，必须与v的类型 一致。
//        unit.foldByKey()

//        unit.sortByKey().foreach(println)




    }

}
