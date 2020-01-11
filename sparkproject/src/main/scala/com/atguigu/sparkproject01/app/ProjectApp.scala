package com.atguigu.sparkproject01.app

import com.atguigu.sparkproject01.bean.{CategoryCountInfo, userVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-10 14:14
 */
object ProjectApp {
    def main(args: Array[String]): Unit = {
        // 1读取数据
        val conf: SparkConf = new SparkConf()
            .setAppName("project01")
            .setMaster("local[2]")
        val context: SparkContext = new SparkContext(conf)
//            context.setLogLevel("error")
        val sourceRDD: RDD[String] = context.textFile("F:/BigData/Spark/user_visit_action.txt")  // 路径
        // 1.1封装数据
        val userVisitActionRDD: RDD[userVisitAction] = sourceRDD.map {
            line =>{
                val splitsstring: Array[String] = line.split("_")
                userVisitAction(
                    splitsstring(0),
                    splitsstring(1).toLong,
                    splitsstring(2),
                    splitsstring(3).toLong,
                    splitsstring(4),
                    splitsstring(5),
                    splitsstring(6).toLong,
                    splitsstring(7).toLong,
                    splitsstring(8),
                    splitsstring(9),
                    splitsstring(10),
                    splitsstring(11),
                    splitsstring(12).toLong)}
        }
            //数据清洗
            //todo 2  需求1：top10 热门品类
                val categoryTop10: Array[CategoryCountInfo] = CategoryTopApp.statCategoryTop10(context,userVisitActionRDD )
// todo 需求1 categoryTop10.foreach(println)
    //        CategoryCountInfo(15,6120,1672,1259)
//        CategoryCountInfo(2,6119,1767,1196)
//        CategoryCountInfo(20,6098,1776,1244)
//        CategoryCountInfo(12,6095,1740,1218)
//        CategoryCountInfo(11,6093,1781,1202)
//        CategoryCountInfo(17,6079,1752,1231)
//        CategoryCountInfo(7,6074,1796,1252)
//        CategoryCountInfo(9,6045,1736,1230)
//        CategoryCountInfo(19,6044,1722,1158)
//        CategoryCountInfo(13,6036,1781,1161)

            //todo 2.2 需求2：top10热门品类中，每个品类的top10 Session(用户) .
       CategoryTop10SessionApp.calcCategorySessionTop10(context,categoryTop10,userVisitActionRDD)



        context.stop()



    }

}
