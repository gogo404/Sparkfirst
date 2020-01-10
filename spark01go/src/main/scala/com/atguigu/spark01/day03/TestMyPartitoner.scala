package com.atguigu.spark01.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-08 20:56
 */
object TestMyPartitoner {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("mypartioner").setMaster("local[2]")
        val context:SparkContext = new SparkContext(conf)

        val unit: RDD[Int] = context.parallelize(List(2,4,6,8,10))
        val unit1: RDD[(Int, Int)] = unit.map((_,1))
//        println(unit.collect().mkString(","))
// TODO: hashPartitioner 有可能 会出现严重的数据倾斜。
                                                // todo 通过equals()方法，判断相同，不会再次分区。如果没有重写equals方法，则会再次执行相同的分区操作。
        val unit2: RDD[(Int, Int)] = unit1.partitionBy(new MyPartitioner(2)).partitionBy(new MyPartitioner(2))
        unit2.glom().collect().foreach(
            q => println(q.mkString(","))
         )
        context.stop()


    }

}
class MyPartitioner(val partitionNum:Int) extends Partitioner{
    // 分区的个数
    override def numPartitions: Int = partitionNum
    // 根据key计算，k-v 应该进入那个分区中。
    override def getPartition(key: Any): Int = key match {
        case null => 0
        case _=> key.hashCode().abs % partitionNum

    }

    override def equals(obj: Any): Boolean = obj match {
        case p:MyPartitioner => p.partitionNum == partitionNum
        case _=> false
    }

}
