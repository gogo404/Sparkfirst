package com.atguigu.spark01.day05

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author gogo
 * @create 2020-01-14 14:10
 */
/* TODO 使用外置 Hive：
*   hive-site.xml文件拷贝
*   MySQL驱动拷贝
*   maven 依赖导入
*   (SparkSession 默认只支持内置Hive，使用外置hive还要，加上 .enableHiveSupport())
*   */
            // todo 1 用外置Hive读数据
object ExtennalHive_spark_read {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession =SparkSession
            .builder()
            .master("local[2]")
            .appName("extenalhivespark")
            .enableHiveSupport()  // todo 支持外置hive的一个步骤  。。 不加外置Hive支持，默认去本地Sparkwarehouse找寻表
            .getOrCreate()
        import ss.implicits._   // todo 导入类，隐式转换，必导。(不管用不用都要导入，没用可以再删。官方推荐)

        ss.sql("use gmall")
        ss.sql("show tables").show()

        ss.close()  // todo 每次记得关闭Session
    }
}
        // todo 2 通过外置Hive写数据
            // todo总结：创建数据库，直接hive中创建，不然默认会创建在本地。
object ExtennalHive_spark_write{
    def main(args: Array[String]): Unit = {
        // todo 每次都要配置一个有权限的用户，向Hive仓库写数据。(永久解决问题： 默认是Lzo用户，只有读的权限，或者配置数据使其具有权限)
      System.setProperty("HADOOP_USER_HOME","atguigu")
        val ss: SparkSession =SparkSession
            .builder()
            .master("local[2]")
            .appName("extenalhivespark")
            // todo 支持外置hive的一个步骤  。。 不加外置Hive支持，默认去本地Sparkwarehouse找寻表
            .enableHiveSupport()
            // todo 建议直接在hive中创建数据库。指定数据库  存放的目录   // 如果不指定，建数据库就会建立在本地。就会gg，显得非常乱、麻烦。
            .config("spark.sql.warehouse.dir","hdfs://hadoop105:9000/user/hive/warehouse/")
            // todo 想从hive中可以看到查看 从spark创建表的表信息。将此设置改为false
                    // todo 遇到的一个问题：使用savaAsTable 或者 insertInto方式 创建的表(文件)在HDFS上以parquet格式保存，
            //              而默认是 convertMetastoreParquet为true，hive无法读取parquet，就会导致建表成功，操作成功，在Idea操作一切正常，但是在hive中只能看到表名，而看不到任何其中的数据。
            //            （这个问题之所以显得怪异，是因为只有数据以parquet格式报存时才会出现这种问题。而若是以SQL语句操作创建表格，修改数据，文件格式不是parquet所以这种问题又不会存在。）
            //                      解决方法：将convertMetastoreParquet设置为false。(涉及了一点hive中数据结构的知识：hive是不敏感的所有数据可以为空，parquet是敏感的，对数据有所要求。在hive中创建以parquet格式存储的文件，同样不会出现这种问题。)
            .config("spark.sql.hive.convertMetastoreParquet",false)
            .getOrCreate()
        import ss.implicits._

       val df1: DataFrame = Seq(("zhangsan",20),("zhangsi",21),("zhangzhang",48)).toDF("name","age")
//       todo 0.1写入数据的方式1 ： df1.write.saveAsTable("user0830test1")  // 自动创建表，必须不存在。

//        todo 0.2 写入数据的方式2： df1.write.insertInto("user0830test1") // todo (insertInto : 忽略列名，仅仅通过位置进行插入处理 )插入的表必须存在，不存在报错
//        df1.write.mode("append").saveAsTable("user0830test1") // todo (savaAsTable: 基于列名进行改变， 与位置无关。)append(追加模式，列名要保持一致)
// todo 0.3 写入数据的方式3：
         // sql 写的格式：1
//        ss.sql("use gmall")
//        ss.sql("create table p1(name string, age int)")
//        ss.sql("insert into table p1 values('abc',10)")
//        ss.sql("select * from gmall.p1").show()

//        df1.createOrReplaceTempView("pp") // 创建视图
        // sql 写的格式：2
//        ss.sql(
//            """
//              |select * from gmall.p1
//              """.stripMargin).show()
        ss.sql("create database sparktest1")
        ss.close()

    }

}