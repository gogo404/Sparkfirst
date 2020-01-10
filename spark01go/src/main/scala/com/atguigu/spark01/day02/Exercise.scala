package com.atguigu.spark01.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-07 15:37
 */
object Exercise {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("exercise")
        val context: SparkContext = new SparkContext(conf)
        val unit: RDD[String] = context.textFile("F:\\BigData\\Spark\\agent.log")
//        时间戳，省份，城市，用户，广告，字段使用空格分割
        val unit1: RDD[Array[String]] = unit.map(_.split(" "))
         val unit2: RDD[((String, String), Int)] = unit1.map(
             args=> ((args(1),args(4)),1)
         )
         val unit3: RDD[((String, String), Int)] = unit2.reduceByKey(_+_)
        val unit4: RDD[(String, (String, Int))] = unit3.map {
            case ((pro, ads), sum) => ((pro, (ads, sum)))
        }
       val unit5: RDD[(String, Iterable[(String, Int)])] = unit4.groupByKey()
        val unit6: RDD[(String, List[(String, Int)])] = unit5.mapValues {
            it => it.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
        }
        unit6.sortBy(_._1.toInt).collect.foreach(println)
//        (0,List((2,29), (24,25), (26,24)))
//        (1,List((3,25), (6,23), (5,22)))
//        (2,List((6,24), (21,23), (29,20)))
//        (3,List((14,28), (28,27), (22,25)))
//        (4,List((12,25), (2,22), (16,22)))
//        (5,List((14,26), (21,21), (12,21)))
//        (6,List((16,23), (24,21), (22,20)))
//        (7,List((16,26), (26,25), (1,23)))
//        (8,List((2,27), (20,23), (11,22)))
//        (9,List((1,31), (28,21), (0,20)))

    }

}
object Exercise12{
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("exercise12")
        val context: SparkContext = new SparkContext(conf)
//        原始： 时间戳，省份，城市，用户，广告，字段使用空格分割
//        需求:  统计出每一个省份广告被点击次数的 TOP3   // (省份,((广告1，次数),(广告2，次数),(广告3，次数)))
        val unit: RDD[String] = context.textFile("F:\\BigData\\Spark/agent.log")
        val unit1: RDD[Array[String]] = unit.map(_.split(" "))
        // 1 得到格式： ((province ,advertisement),one)
        val proadsAndone: RDD[((String, String), Int)] = unit1.map {
            args => ((args(1), args(4)), 1)
        }
        // 2 聚合，格式变动
        val proAndadcount: RDD[(String, (String, Int))] = proadsAndone.reduceByKey(_ + _).map {
            case ((pro, ad), countsum) => (pro, (ad, countsum))
        }
        // 3 快要得到最终格式哈哈： (pro,List((ad1,count),(ad2,count),(ad3,count)))
        // TODO 将每个省的广告分为一组
        val proAnditeratoradcount: RDD[(String, Iterable[(String, Int)])] = proAndadcount.groupByKey()
        // 4 todo 组内排序所以要先map，iterable迭代器(迭代器中的元素并不存在，只有当实际调用时，调用那批将那批写入内存。)，所以要排序必须先转为List结构。
                // sortBy (这里的sortBy、take都是scala中的)默认为升序
        val proAndlistadcount: RDD[(String, List[(String, Int)])] = proAnditeratoradcount.mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
        // 5 出于美观考虑： 最终结果按省份排序
        val zuizhong: RDD[(String, List[(String, Int)])] = proAndlistadcount.sortBy(_._1.toInt)
        zuizhong.collect.foreach(println)
//        (0,List((2,29), (24,25), (26,24)))
//        (1,List((3,25), (6,23), (5,22)))
//        (2,List((6,24), (21,23), (29,20)))
//        (3,List((14,28), (28,27), (22,25)))
//        (4,List((12,25), (2,22), (16,22)))
//        (5,List((14,26), (21,21), (12,21)))
//        (6,List((16,23), (24,21), (22,20)))
//        (7,List((16,26), (26,25), (1,23)))
//        (8,List((2,27), (20,23), (11,22)))
//        (9,List((1,31), (28,21), (0,20)))












    }
}
