package com.atguigu.spark01.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-08 21:24
 */
object TestTransmitFun {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testtransmit")
        val context: SparkContext = new SparkContext(conf)
        val unit: RDD[(String, Int)] = context.parallelize(Array(("zhangsan",1),("zhangsi",2),("zhangwu",3),("zhangliu",4),("zhagqi",5)))
         val unit2: RDD[String] = context.parallelize(Array(("zhangsan"),("zhangsi"),("zhangwu"),("zhangliu"),("zhagqi")))
        // TODO: RDD  算子中用到对象的属性或者方法时，对象需要序列化
                //todo transmit method 方法
                //todo transmit function 函数
        // 如果是传递局部遍历，则不用考虑对象的序列化

        //        println(new Searcher("zhang").query)
                val s1: Searcher = new Searcher("zhang") //create  at driver ,but
//        s1.getMatchRDD1(unit2).foreach(println)   //todo extends Serializable // execute in executor , but executor donot have the object's function and variable
//                s1.getMatchRDD2(unit2).foreach(println) //todo extends Serializable
//        s1.getMatchRDD3("zh",unit2 ) // todo 局部变量 neednot extends Serializable




    }

}
// 需求： 在RDD中查找出来包含query的字符串的元素
class Searcher(val query:String) extends Serializable {
    def isMatch(s:String) :Boolean={
        s.contains(query)
    }
//    transmit func : 过滤出包含query 字符串的字符串组成新的RDD
    def getMatchRDD1(rdd:RDD[String]) ={
        rdd.filter(isMatch)
    }
    // transmit variable : 过滤出包含query 字符串的字符串组成新的RDD
    def getMatchRDD2(rdd:RDD[String])={
        rdd.filter(_.contains(query))
    }
    def getMatchRDD3(a:String,rdd:RDD[String])={
//        println(a)
        rdd.filter(s=> s.contains(a))

    }
}
