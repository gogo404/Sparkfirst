package com.atguigu.sparkproject01.app

import com.atguigu.sparkproject01.bean.{CategoryCountInfo, userVisitAction}
import org.apache.hadoop.mapred.TaskLog.LogName
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author shkstart
 * @create 2020-01-10 16:13
 */
object CategoryTop10SessionApp {

        def calcCategorySessionTop10(sc:SparkContext,categoryTop10:Array[CategoryCountInfo],userVisitActionRDD:RDD[userVisitAction]) = {
            //过滤前10 品类id，的点击记录。
            val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
            val filteruserVisitAction: RDD[userVisitAction] = userVisitActionRDD.filter(
                action => cids.contains(action.click_category_id))
            var result: Map[Long, List[(String, Int)]] = Map[Long, List[(String, Int)]]()

            for (cid <- cids) {
                val cidRDDs: RDD[userVisitAction] = filteruserVisitAction.filter(_.click_category_id == cid)
                 result = cidRDDs
                    .map {
                    action => ((action.click_category_id, action.session_id), 1)
                    }
                    .reduceByKey(_ + _)
                    .sortBy(-_._2)
                    .map {
                        case ((cid, sid), count) => (cid, (sid, count))
                    }
                    .take(10)
                    .groupBy(_._1)
                    .mapValues {
                        case aa => aa.map(_._2).toList
                    }
                println(result.toList)
            }


        }
}
