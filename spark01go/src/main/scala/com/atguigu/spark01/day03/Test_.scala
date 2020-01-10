package com.atguigu.spark01.day03

/**
 * @author shkstart
 * @create 2020-01-08 18:22
 */
object Test_ {
    def main(args: Array[String]): Unit = {
        //TODO _ 的广泛应用
        //todo 1 导类时的通配符（类似java中的* ）
        import java.lang._
        //todo 2 元组索引的前缀
        ("zhangsan", 2)._2
        //todo 3 函数值的隐式参数
        List(1,2,3).map(_*2)   // 等价于
        List(1,2,3).map(a => a*2)
        //todo 4 使用默认值初始化字段
        class go{
            var name:String =_
        }
//        println(new go().name) // ==> null
        //todo 5 函数名中混合操作符(函数名为：foo_: )
        def foo_: (a:Int):Int ={// Scala 不允许直接使用字母和数字字 符的操作符
            1
        }
//        println(foo_:(2)) // ==> 1
        //todo 6 模式匹配的通配符

//        val operator :Char ='d'
//        val result = operator match {
//            case '+' => println(1)
//            case _ => println(2)
//        }
        //todo 7 异常处理，catch在代码块中和case联用
//        try {
//            println(new Exception)
//        } catch {
//            case  OutOfMemoryError => println("OutOfMemoryError")
//            case Exception => println("Exception")
//        } finally {
//            println("goto finally")
//        }
        //todo 8 (可变形参)作为分解操作的部分,
//              将数组或者列表参数传递给可变长度参数的函数前，将其分解为离散的值。
//        case Array(0, _*) => "以0开头的数组" //匹配以0开头和数组
        //todo 9 用于部分应用一个函数。
            // todo 将一个函数作为整体传递。
//       def gg1(): Unit ={
//           def gg2(): Unit ={
//
//           }
//           gg2()_
//       }
//        val gg = gg1()_






    }

}
