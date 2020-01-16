package com.atguigu.sparkstreaming.sparkstreaming01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * @author gogo
 * @create 2020-01-13 18:22
 */
// TODO 自定义数据源  (其实是自定义接收器，读取数据源的数据)，即自写 Receiver
object MyReceiverDemeo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("myReceiver1")
        // todo 入口对象创建完成
        val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
        // todo  数据通过自定义的接收器读出来了
        val RIDStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop105",10000) )

        RIDStream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_).print(100)
        ssc.start()
        ssc.awaitTermination()
    }
}
// TODO 自定义接收器(传入主机名和端口号)
    // todo socket 实现数据的接受和写出
class MyReceiver(host : String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
    var socket: Socket = _
    var reader1: BufferedReader = _
    // 启动，开始从数据源读取数据
    override def onStart(): Unit = {

        runThread {  //控制抽象，调用。(将下列代码放入一个线程中执行。)
            try {
                // todo 1 创建Socket对象
                socket = new Socket(host, port)
                // todo 2 读入数据
                //           todo 字节输入流(直接读文件，读取的基本单位字节)==》 (因为数据源的数据一般都是String类型，而且考虑大的数据量的话)
                //                  所以转换成字符输入流(通过缓存读取数据，读取的基本单位是一个字符)
//         socket.getInputStream
                //          todo 字符输入流 涉及流之间的转换，所以最好指定编码，代码可读性强，且避免出错。(读入的是一个一个的字符)
//        new InputStreamReader(socket.getInputStream,"utf-8")
                //          todo 读入一个一个的字符，读入太慢，且不是想要的数据结构 ，再转成缓冲流BufferedReader
                reader1 = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
                //          todo 利用缓冲流，一行一行的读取(得到字符串)
                var str: String = reader1.readLine()
                // todo 3 将数据发送到其他executor 上。  同时，循环以实现流式数据的不断读入
                while (socket.isConnected && str != null) {
                    store(str)    // 唯一用到spark方法的地方
                    str = reader1.readLine() // 持续性读入
                    // errro   str 定义成val （顺便说一句：编程原则：能定义成常量不要定义成变量）
                }
            } catch {
                case e => println(e.getMessage)

            } finally {
                restart("重启receiver.......")  // 一旦出错，重新连接。
            }
        }
    }
    // 关闭，释放资源
    override def onStop(): Unit = {
        //避免空指针异常
        if(socket != null){
            socket.close()
        }
        if(reader1 != null){
            reader1.close()
        }
    }
    def runThread(f: => Unit) ={   // 控制抽象
        new Thread(){
            override  def run()= f
        }.start()
    }
}
