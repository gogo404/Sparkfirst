package com.atguigu.spark01.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-07 20:37
 */
object TestMethod01 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("method01")
        val context: SparkContext = new SparkContext(conf)

        val rdd1: RDD[(String, Int)] = context.parallelize(List(("a",2),("b",4),("c",6),("a",3),("b",2),("c",12),("a",22)))
        val rdd11: RDD[(String, Int)] = context.parallelize(List(("a",22),("b",0),("c",6),("a",22)))
//      todo   def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
    //        val rdd2: RDD[(String, Int)] = rdd1.foldByKey(0)(_+_)
    //        rdd2.foreach(println)
        // todo def combineByKey[C](
        //      createCombiner: V => C,     // 对比零值： 通过计算得到的，零值不是写死的。(碰到第一个key ，计算得到零值)
        //      mergeValue: (C, V) => C,  // 分区内的聚合
        //      mergeCombiners: (C, C) => C): RDD[(K, C)]  //分区间的聚合op
//        val rdd2: RDD[(String, Int)] = rdd1.combineByKey(x=>x,(_:Int)+(_:Int),(_:Int)+(_:Int))
//        rdd2.foreach(println) // 完成合并

        // todo cogroup 先对rdd1、rdd2 分别分组，然后在直接组合在一起(不会丢元素)。(b,(CompactBuffer(4, 2),CompactBuffer(0)))
//        val rdd2: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd11)
//        (b,(CompactBuffer(4, 2),CompactBuffer(0)))(a,(CompactBuffer(2, 3, 22),CompactBuffer(22, 22)))(c,(CompactBuffer(6, 12),CompactBuffer(6)))
        //TODO aggregateByKey
//        val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(0)(_+_,_+_)
val rdd = context.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        // 需求1 求分区内最大值，然后求和
//        val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)(
//           {case (x,y) => x.max(y)},{case (g,h) => g+h}
//        )
        // 需求2 同时求分区最大值和最小值，然后求和
//        val rdd2: RDD[(String, (Int, Int))] = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
//            {
//                case ((x, y), z) => (x.max(z), y.min(z))
//            },
//            {
//                case ((g, h), (z, t)) => (g + z, h + t)
//            }
//        )
        // 求每个key的平均值
//        val rdd2: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
//            {
//                case ((sum, count), z) => (sum + z, count + 1)
//            },
//            {
//                case ((su, cou), (su2, cou2)) => ((su + su2) , cou + cou2)
//            }
//        )
//        val rdd3: RDD[(String, Int)] = rdd2.mapValues {
//            case (su, co) => su / co
//        }
        // todo coalesce  减少分区数一般用。
//        rdd.coalesce(2)

//        rdd4.takeOrdered(3)(Ordering.Int.reverse)
//        val rdd2: (((String, Int), (String, Int)) => (String, Int)) => (String, Int) = rdd.reduce(_)
//            rdd.aggregate(0)(_.toInt.max(_.),_+_)











    }

}
