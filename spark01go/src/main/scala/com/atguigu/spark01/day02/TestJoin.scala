package com.atguigu.spark01.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-07 19:47
 */
object TestJoin {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testjoin")
        val context:SparkContext = new SparkContext(conf)

        val rdd1: RDD[(Int, String)] = context.makeRDD(List((1, "a"), (2, "b"), (3, "a"), (4, "b"), (5, "aa"), (8, "b")))
        val rdd2: RDD[(Int, String)] = context.makeRDD(List((1,"a"),(3,"a"),(8,"bb"),(8,"a"),(8,"aa"),(9,"a"),(10,"a")))
        val rdd3: RDD[(Int, String)] = context.parallelize(Array((1,"a"),(2,"b"),(1,"c")))
        val rdd4: RDD[(Int, String)] = context.parallelize(Array((1,"bb"),(2,"cc"),(6,"cc")))

//        rdd1.join(rdd2).collect.foreach(println)  // todo 内连接
        // (8,(b,bb))(8,(b,a))(8,(b,aa))(1,(a,a))(3,(a,a))
//        rdd1.leftOuterJoin(rdd2).collect().foreach(println) // todo 左外连接 有可能出现空值，所以类型为option(None,Some(String,None/Some))
//        (4,(b,None))(8,(b,Some(bb)))(8,(b,Some(a)))(8,(b,Some(aa)))(2,(b,None))(1,(a,Some(a)))(3,(a,Some(a)))(5,(aa,None))
//        rdd1.rightOuterJoin(rdd2).collect().foreach(println) // 右外连接 todo 注意看结果型式： (1,(Some(a),a))(3,(Some(a),a))
//        rdd3.fullOuterJoin(rdd4).foreach(println) //todo 满外连接： (6,(None,Some(cc)))(1,(Some(a),Some(bb)))(1,(Some(c),Some(bb)))











    }

}
