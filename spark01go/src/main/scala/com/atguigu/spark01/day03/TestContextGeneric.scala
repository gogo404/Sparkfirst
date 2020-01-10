package com.atguigu.spark01.day03

/**
 * @author shkstart
 * @create 2020-01-08 8:53
 */
object TestContextGeneric {
    def main(args: Array[String]): Unit = {
        // TODO [T:K]上下文泛型
            // todo 1
//        println(max("a", "b")(Ordering.String))  // AnyRef
//        println(max(1,2))  // error  // AnyVal
//        def max[T <: Comparable[T]] (a:T,b:T)(ord:Ordering[T]): T ={
////            if (a.compareTo(b) >0) a else b
//            if (ord.gteq(a,b)) a else b
//        }
        // todo 2
////        println(max("a", "b")(Ordering.String))  // AnyRef
//        println(max(1,2))    // AnyVal
//        def max[T] (a:T,b:T)(implicit ord:Ordering[T]): T ={
////            if (a.compareTo(b) >0) a else b
//            if (ord.gteq(a,b)) a else b
//        }
        //todo 3
//        println(max("a", "b")(Or
//        al
            implicit val ord:Ordering[user] = new Ordering[user]{
                override def compare(x: user, y: user) = x.age-y.age
            }
        println(max(user(1, "go"), user(2, "goto")))

        // 简写，必须有隐式值   Ordering[T]
        def max[T:Ordering] (a:T,b:T): T ={
//            if (a.compareTo(b) >0) a else b
                        //implicitly  在Predef中
                                //todo 召唤隐式值  Ordering[T]
            val ord: Ordering[T] = implicitly [Ordering[T]]

            if (ord.gteq(a,b)) a else b
        }

case class user(age:Int,name:String){
}

    }

}

object TestContextGeneric02 {
    def main(args: Array[String]): Unit = {
        // TODO: 1
//        def maxgg1[T](a:T,b:T)(ord: Ordering[T]): Unit ={
//            println("a>b " + ord.gteq(a, b))
//        }
//        maxgg1("a","b"
//        )(Ordering.String)
//        maxgg1(1,2)(Ordering.Int)
// TODO: 2
//        def maxgg1[T](a:T,b:T)(implicit  ord: Ordering[T]): Unit ={
//            println("a>b " + ord.gteq(a, b))
//        }
//        maxgg1("a","b")
//        maxgg1(2,11)
// TODO: 3
//todo 意为：必须有一个隐式参数Ordering[T]
def maxgg1[T: Ordering](a: T, b: T): Boolean = {
    //todo 召唤隐式参数Ordering[T]
    val ord: Ordering[T] = implicitly[Ordering[T]]
    println("a>b " + ord.gteq(a, b))
    ord.gteq(a, b)
//    if (ord.gteq(a, b)) a else  b
}
        implicit val ord: Ordering[user2] = new Ordering[user2] {
            override def compare(x: user2, y: user2) = x.age - y.age
        }

        maxgg1(new user2(1, "zhangsan"), new user2(2, "zhangsi"))

    }
}
case class user2(age:Int,name:String ){}