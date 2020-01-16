package com.atguigu.sparkstreaming.kafka.WordCountWindow

import java.sql.{DriverManager, PreparedStatement}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author gogo
 * @create 2020-01-15 18:09
 */
object Transformwithnostates {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("Transformwithnostates")
        val context: StreamingContext = new StreamingContext(conf,Seconds(2))

        val sourceStream: ReceiverInputDStream[String] = context.socketTextStream("hadoop105",9999)
        val ds1: DStream[(String, Int)] = sourceStream.transform {
            rdd => rdd.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
        }

        ds1.print(100)
        val driver = "com.mysql.jdbc.Driver"
        val url= "jdbc:mysql://hadoop106:3306/rdd"  // todo 链接及 库名
        val username = "root"
        val password = "root"
        // 写入到MySQL中。
        ds1.foreachRDD{
            rdd => rdd.foreachPartition{ it => {
                Class.forName(driver)
                val conn = DriverManager.getConnection(url,username,password)
                it.foreach {x =>{
                    val statement:PreparedStatement =
                        conn.prepareStatement("insert into user values(?,?)")
                    statement.setString(2,x._1)
                    statement.executeLargeUpdate()
                }
                }
            }
            }
        }


        context.start()
        context.awaitTermination()
    }
}
