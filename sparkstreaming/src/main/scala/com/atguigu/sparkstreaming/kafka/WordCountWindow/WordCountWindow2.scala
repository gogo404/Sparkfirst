package com.atguigu.sparkstreaming.kafka.WordCountWindow

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author gogo
 * @create 2020-01-15 20:48
 */
// TODO: 优化的 窗口函数运用
        // todo  只有聚合的时候窗口才有意义。
object WordCountWindow2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("WordCountWindow2")
            .setMaster("local[2]")
                                                            //todo 时间间隔在这哈哈哈
        val context :StreamingContext= new StreamingContext(conf,Seconds(2))
        //todo 这里是做优化用的。 避免窗口之间重复的运算。需要用到检查点保存数据。
        //          todo 存在小的bug，无论代码发生任何问题，这里的checkpoint dir都必须重新设置，或者删掉，不然会出现问题。无法通过其他方式解决。
        context.checkpoint("./ck1")
        val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop105",9999)

        val result1: DStream[(String, Int)] = ds1.flatMap(_.split("\\W+"))
            .map((_, 1))
//               (func,窗口长度,滑动长度)  todo  这里窗口长度和滑动长度都必须是时间间隔的整数倍
//            .reduceByKeyAndWindow(_+_, Seconds(12))
//                .reduceByKeyAndWindow(_+_,Seconds(10),Seconds(6))//todo error 有两个这样的函数，混淆不清，不知道要调用哪一个。可通过命名参数解决。
//                .reduceByKeyAndWindow(_+_,Seconds(10),slideDuration = Seconds(6))
        // 还有一个窗口函数 ：groupByKeyWindow()
            // TODO: def reduceByKeyAndWindow(
            //      reduceFunc: (V, V) => V,
            //      invReduceFunc: (V, V) => V,  // invReduceFunc 处理的是窗口间数据，这个函数就是做优化的。
            //      windowDuration: Duration,
            //      slideDuration: Duration = self.slideDuration,
            //      numPartitions: Int = ssc.sc.defaultParallelism,
            //      filterFunc: ((K, V)) => Boolean = null //如果窗口间数据处理用减法，之前存在的数据会生成零元组(或者其他结构)，一直不会消失，bug
            //    ): DStream[(K, V)]

            // TODO (_+_,_-_,Seconds(10),Seconds(6)) : 分析： 同一窗口间数据value相加；跨窗口的数据 新增的部分 减去 移走的部分；窗口长度； 滑动长度
                                // TODO:  invReduceFunc = _-_ ： 意思是：新增的部分 减去 移走的部分(新窗口滑动一个长度，这个长度的本窗口的数据就是移走的部分)

                .reduceByKeyAndWindow(_+_,_-_,Seconds(10),Seconds(6))
        result1.print(100)

        context.start()
        context.awaitTermination()
    }

}
