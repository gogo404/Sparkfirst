package com.atguigu.sparkproject01.app

import com.atguigu.sparkproject01.bean.{CategoryCountInfo, SessionInfo, userVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable


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

// TODO:        用Spark的sortBy，取代scala的sort，这样就不用转换为List，避免OOM。
            for (cid <- cids) {
                val cidRDDs: RDD[userVisitAction] = filteruserVisitAction.filter(_.click_category_id == cid)
                val result: Map[Long, List[(String, Int)]] = cidRDDs
                    .map {
                        action => ((action.click_category_id, action.session_id), 1)
                    }
                    .reduceByKey(_ + _)
                    .sortBy(-_._2)
                    .map {
                        case ((cid, sid), count) => (cid, (sid, count))
                    }
                    .take(10)
                    .groupBy(_._1)    //将同cid数据放到一起，方便以后的处理，或者美化的考虑。
                    .mapValues {
                        case aa => aa.map(_._2).toList
                    }

                println(result.toList)
            }

        }
}
object CategoryTop10SessionApp2 {
    // TODO: Treeset 解决OOM和Job过多，效率不高的问题。
    def calcCategorySessionTop10(sc:SparkContext,categoryTop10:Array[CategoryCountInfo],userVisitActionRDD:RDD[userVisitAction]) = {
        //过滤前10 品类id，的点击记录。
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteruserVisitAction: RDD[userVisitAction] = userVisitActionRDD.filter(
            action => cids.contains(action.click_category_id))
        val cidsidAndone: RDD[((Long, String), Int)] = filteruserVisitAction.map {
            aa => ((aa.click_category_id, aa.session_id), 1)
        }
        // 根据（cid,sid）聚合，转换为（cid,（sid,count））结构，方便后续自定义分区过程
        val cidAndsidcount: RDD[(Long, (String, Int))] = cidsidAndone.reduceByKey(new CategoryPartitioner(cids),_ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        //
        val result3: RDD[(Long, SessionInfo)] = cidAndsidcount.mapPartitions(it => {
            var set = new mutable.TreeSet[SessionInfo]()
            var categoryId = 0L
            it.foreach {
                case (cid, (sid, count)) =>
                    categoryId = cid
                    set += SessionInfo(sid, count)
                    if (set.size > 10) set = set.take(10)
            }
            set.map((categoryId, _)).toIterator
        })
        result3.collect.foreach(println)

    }
}
object CategoryTop10SessionApp2_2 {
    // TODO: Treeset 解决OOM和Job过多，效率不高的问题。
    def calcCategorySessionTop10(sc:SparkContext,categoryTop10:Array[CategoryCountInfo],userVisitActionRDD:RDD[userVisitAction]) = {
        //
        val cids: Array[String] = categoryTop10.map(_.categoryId)
        val top10cids: RDD[userVisitAction] = userVisitActionRDD.filter {
            aa => cids.contains(aa.click_category_id)
        }
        val cidsidAndone: RDD[((Long, String), Int)] = top10cids.map {
            aa => ((aa.click_category_id, aa.session_id), 1)
        }
        // 完成聚合，结构准备好
        val cidAndsidcount: RDD[(Long, (String, Int))] = cidsidAndone.reduceByKey(new CategoryPartitioner(cids), _ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }

        cidAndsidcount.mapPartitions{
         val set = new mutable.TreeSet[SessionInfo]()
            var categoryid :String = ""
            it => it.foreach{
                case (cid,(sid,count)) =>

            }
        }
    }
}

class CategoryPartitioner(cids:Array[String]) extends Partitioner{
    private val map : Map[String,Int]  = cids.zipWithIndex.toMap
    override def numPartitions: Int = cids.length

    override def getPartition(key: Any): Int = key match {
        case (k:String,_) => map(k)
    }
}
// 自定义分区器，根据cid分区，减少一次shuffle过程，提高效率。
class CategoryPartitioner(cids:Array[Long]) extends Partitioner{
    private val map : Map[Long,Int]  = cids.zipWithIndex.toMap
    override def numPartitions: Int = cids.length

    override def getPartition(key: Any): Int = key match {
        case (k:Long,_) => map(k)
    }
}