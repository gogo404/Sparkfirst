package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, streaming}
/**
 * @author gogo
 * @create 2020-01-16 9:08
 *    主程序
 */
object RealtimeApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeApp")
        val ssc = new StreamingContext(conf, streaming.Seconds(5))
        ssc.checkpoint("realtime")
        val sourceDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log")

        val adsInfoDSteam: DStream[AdsInfo] = sourceDStream.map(record => {
            // 1576655451922,华北,北京,105,2
            val split: Array[String] = record.value().split(",")
            AdsInfo(
                split(0).toLong,
                split(1),
                split(2),
                split(3),
                split(4))
        })
        // 需求1: 每天每地区没广告的点击率的top3
        areadayadsTop3.getareadayadstop3(adsInfoDSteam).print(1000)

        ssc.start()
        ssc.awaitTermination()

    }
}
