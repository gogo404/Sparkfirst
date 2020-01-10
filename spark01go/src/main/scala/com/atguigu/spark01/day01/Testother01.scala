package com.atguigu.spark01.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-06 20:54
 */
object Testother01 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("other01")
        val context: SparkContext = new SparkContext(conf)

        val list1: List[Int] = List(1,1,1,2,2,2,2,3,3,3,3,4,5,5,5,5)
        val list2: List[String] = List("zhangsan","sanzhang","aaa","abc")
        val rdd1: RDD[Int] = context.parallelize(list1,4)
        val rdd2: RDD[String] = context.parallelize(list2,4)
// todo 1 glom() 将每个分区元素合并为一个数组，类型为Array
    //        rdd1.glom().collect.foreach(println)
// todo 2 groupBy()  启动shuffle，产生大量磁盘IO，性能降低，慎用。
    //        rdd2.flatMap(x=>x).groupBy(x=> x).foreach(println)
//        val unit: RDD[Array[String]] = rdd2.flatMap(x => Array(Array(x), Array(x + 1)))
//        unit.foreach(println)//todo flatMap() 方法既能增多元素，也能使元素数量减少

// todo 3 filter() 过滤
//        rdd1.filter(_>3).foreach(println)
// todo 4 sample(withreplacement，fraction，seed) 抽样，不太准
            // seed一般不指定，指定就不是随机的了。
        // false 不放回抽样，fraction[0,1]
//        rdd1.sample(false,0.4).foreach(println)
        // true 放回抽样，fraction[0,+∞)
//        rdd1.sample(true,1.3).foreach(println)
//  todo 5 distinct()
//        rdd1.distinct()
//        1. 先看hashcode
//            2. 再是否为同一个对象
//            3. 然后再看equals
    //      val list:List[go] = List(go(1,"zhangsan"),go(1,"zhangsi"),go(1,"zhangsan"),go(2,"zhangsi"))
//        context.parallelize(list).distinct.foreach(println) // 样例类实现了comparable接口
        rdd1.coalesce(2).foreach(println)

    }

}
case  class go(age:Int,name:String){

}
class goto(age:Int,name:String){

}