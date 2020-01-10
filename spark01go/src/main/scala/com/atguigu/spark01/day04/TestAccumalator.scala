package com.atguigu.spark01.day04

import org.apache.spark.util.AccumulatorV2

/**
 * @author shkstart
 * @create 2020-01-10 10:21
 */
object TestAccumalator {
    def main(args: Array[String]): Unit = {


    }

}
class MyLongAccumlator extends AccumulatorV2[Long,Long]{
    override def isZero: Boolean = ???

    override def copy(): AccumulatorV2[Long, Long] = ???

    override def reset(): Unit = ???

    override def add(v: Long): Unit = ???

    override def merge(other: AccumulatorV2[Long, Long]): Unit = ???

    override def value: Long = ???
}
