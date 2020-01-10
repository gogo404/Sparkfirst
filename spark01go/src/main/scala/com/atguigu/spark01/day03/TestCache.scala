package com.atguigu.spark01.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2020-01-09 10:48
 */
object TestCache {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("testcache").setMaster("local[2]")
        val context: SparkContext = new SparkContext(conf)
        context.setCheckpointDir("./sccheckpointdir")
        val rdd: RDD[Int] = context.parallelize(List(1,2,3,4))

        val rdd2: RDD[(Int, Int)] = rdd.map {
            x =>
                println(x + "->")
                (x, 1)
        }
        val rdd3: RDD[(Int, Int)] = rdd2.filter {
            x =>
                println(x + "=>")
                true
        }
        // TODO: cache And checkpoint  None of them : 3
        // todo  only cache      : 1
        // todo  only checkpoint : 2
        // todo  both cache and checkpoint : 1

//        rdd3.cache()  //todo def cache(): this.type = persist()
//        rdd3.checkpoint()
        //todo stronger than cache . can assign the StorageLevel
        rdd3.persist(StorageLevel.MEMORY_ONLY)   // rdd2.cache() // 等价于 rdd2.persist(StorageLevel.MEMORY_ONLY)



        rdd3.collect()
        rdd3.collect()
        rdd3.collect()










    }

}
