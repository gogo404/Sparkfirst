package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
 * @author gogo
 * @create 2020-01-16 9:49
 */
object areadayadsTop3 {
    def getareadayadstop3(adsInfoDSteam: DStream[AdsInfo]) ={
//        AdsInfo(1579139493032,华南,广州,100,2,2020-01-16 09:51:33.032,2020-01-16,09:51)
        val result = adsInfoDSteam.map {
            //           地区，            天，              广告id,        人
            adsinfo1 => ((adsinfo1.area,  adsinfo1.dayString, adsinfo1.adsId), 1)
        }
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
            .map {
                case ((area,  day, adsid), usersum) => ((area, day), (adsid, usersum))
            }.groupByKey()
            .mapValues {
                it => (it.toList.sortBy(_._2).reverse.take(3))
            }
        // 3. 写入到redis
        result.foreachRDD{
            rdd => {
                //建立连接
                val client: Jedis = RedisUtil.getJedisClient
                val arr: Array[((String, String), List[(String, Int)])] = rdd.collect()
                // 写到
                arr.foreach{
                    case ((area, day), list1) => {
                        import org.json4s.JsonDSL._
                        val str: String = JsonMethods.compact(JsonMethods.render(list1))
                        client.hset(s"area:day:top3:$day",area,str)
                    }
                }
                client.close()
            }
        }

       result
    }
}
