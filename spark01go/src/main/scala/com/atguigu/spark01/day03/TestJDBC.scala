package com.atguigu.spark01.day03

import java.sql
import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-08 16:40
 */
object TestJDBCRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testjdbc")
        val context: SparkContext = new SparkContext(conf)

        val driver: String = "com.mysql.jdbc.Driver"
        val url:String = "jdbc:mysql://hadoop106:3306/rdd"
        val username :String = "root"
        val passwd :String  = "root"
        val rdd = new JdbcRDD(
            context,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url,username,passwd)
            },
            "select num , department from user where num >= ? and num <= ? ",
            1,
            200,
            2,
            result => (result.getInt(1),result.getString(2))
        )
        rdd.collect.foreach(println)
        context.stop()
    }
}
object TestJDBCwrite{
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("testjdbcwrite").setMaster("local[2]")
        val context:SparkContext = new SparkContext(conf)

        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop106:3306/rdd"
        val username = "root"
        val passwd = "root"

        val rdd: RDD[(Int,String)] = context.parallelize(Array((110, "plice"), (119, "fire")))
        rdd.foreachPartition(
            it => {
                Class.forName(driver)
                val conn: Connection = DriverManager.getConnection(url,username,passwd)
                it.foreach(
                    x=> {
            val ps: PreparedStatement = conn.prepareStatement("insert  into user values (?,?)")
                        ps.setInt(1,x._1)
                        ps.setString(2,x._2)
                        ps.executeUpdate()
                        ps.close()
                    })
            })
        context.stop()


    }
}
