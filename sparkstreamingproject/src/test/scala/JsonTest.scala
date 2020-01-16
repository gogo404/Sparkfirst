import org.json4s.jackson.JsonMethods

/**
 * @author gogo
 * @create 2020-01-16 9:25
 */
object JsonTest {
    def main(args: Array[String]): Unit = {
        val tuples: List[(String, Int)] = List(("1",2),("3",4))
        import org.json4s.JsonDSL._
        val str: String = JsonMethods.compact(JsonMethods.render(tuples))
        println(str) //[{"1":2},{"3":4}]



    }

}
