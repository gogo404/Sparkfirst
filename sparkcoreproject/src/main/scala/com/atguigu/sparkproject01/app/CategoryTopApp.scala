package com.atguigu.sparkproject01.app

import com.atguigu.sparkproject01.acc.CategoryAcc
import com.atguigu.sparkproject01.bean.{CategoryCountInfo, userVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author gogo
 * @create 2020-01-10 14:24
 */
object CategoryTopApp {
    // 统计  计算
    def statCategoryTop10(sc: SparkContext,userVisitActionRDD :RDD[userVisitAction]):Array[CategoryCountInfo]={
        val acc: CategoryAcc = new CategoryAcc
        sc.register(acc,"CategoryAcc")
    // 变量RDD，计算每个cid的3个指标
        userVisitActionRDD.foreach(action => {
            acc.add(action)
        })
        //Map[("cid","click") -> 1000]
        val cidActionAndCountGrouped: Map[String, Map[(String, String), Long]] = acc.value.groupBy(_._1._1)
        val categoryCountInfos: Array[CategoryCountInfo] = cidActionAndCountGrouped.map {
            case (cid, map) =>
                CategoryCountInfo(cid,
                    map.getOrElse((cid, "click"), 0),
                    map.getOrElse((cid, "order"), 0),
                    map.getOrElse((cid, "pay"), 0))
        }.toArray
        categoryCountInfos
            .sortBy(
            info => (info.clickCount,info.orderCount,info.payCount))(Ordering.Tuple3(Ordering.Long.reverse,Ordering.Long.reverse,Ordering.Long.reverse))

            .take(10)
     }


}
