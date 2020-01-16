import org.apache.spark.sql.SparkSession

/**
 * @author gogo
 * @create 2020-01-13 11:30
 */
object SQLApp {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","gogo") // 临时的，永久的可通过更改配置文件设置

        val sparksesson: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("sqlapp")
            .enableHiveSupport()
            .getOrCreate()
        sparksesson.sql("use sparkpractice")
        //todo 1  取得需要的字段
        sparksesson.sql(
            """
              |select
              |     ci.*,
              |     pi.product_name,
              |     uv.click_product_id
              |from user_visit_action uv
              |join product_info pi on uv.click_product_id = pi.product_id
              |join city_info ci on uv.city_id = ci.city_id
              """.stripMargin).createOrReplaceTempView("t1")

        //1  sparksesson.sql("select * from t1").show()



        sparksesson.close()

    }
}
