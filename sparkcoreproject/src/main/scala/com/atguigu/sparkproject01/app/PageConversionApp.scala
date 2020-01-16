package com.atguigu.sparkproject01.app

import com.atguigu.sparkproject01.bean.userVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author gogo
 * @create 2020-01-11 11:20
 */
object PageConversionApp {
    def statPageConversionRate(sc:SparkContext,userVisitActionRDD:RDD[userVisitAction],page:String): Unit = {
        //todo 1 得到目标页面

        val splits: Array[String] = page.split(",")

        val prepages: Array[String] = splits.slice(0,splits.length-1)
        val postPages: Array[String] = splits.slice(1,splits.length-1)

        val pageFlow: Array[String] = prepages.zip(postPages).map {
            case (prepage, postpage) => s"$prepage->$postpage"
        }
        // todo: 2 分母
        val pageAndCount: collection.Map[Long, Long] = userVisitActionRDD
            .filter(action => prepages.contains(action.page_id.toString))
            .map(action => (action.page_id, 1))
            .countByKey()

        // todo 3 分子

        val sessionRDD: RDD[(String, Iterable[userVisitAction])] = userVisitActionRDD.groupBy(_.session_id)
        val totalPageFlows: collection.Map[String, Long] = sessionRDD.flatMap {
            case (sid, actionid) =>
                val actions: List[userVisitAction] = actionid.toList.sortBy(_.action_time)
                val preActions: List[userVisitAction] = actions.slice(0, actions.size - 1)
                val postActions: List[userVisitAction] = actions.slice(1, actions.size - 1)

                preActions.zip(postActions).map {
                    case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
                }.filter(flow => pageFlow.contains(flow))
            //session 下的跳转流
        }.map((_, 1)).countByKey()

        // 计算跳转流
        val result4: Iterable[Double] = totalPageFlows.map {
            case (pageFlow, count) =>
                count.toDouble / pageAndCount(pageFlow.split("->")(0).toLong)
        }
        result4.foreach(println)
    }
















}
