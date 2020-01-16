package com.atguigu.spark01.day05

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
 * @author gogo
 * @create 2020-01-12 9:00
 */
object userDefinedFunction {
    def main(args: Array[String]): Unit = {
        // TODO: 自定义udf函数
        val session: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("udfDemo1")
            .getOrCreate()
        // tolower:这个暂时不知道怎么用。                               // 直接用的是函数名
        val tolower: UserDefinedFunction = session.udf.register("tolowercase",(s:String)=>{s.toLowerCase()})

//        session.sql("select tolowercase('GOGO')").show()
//        session.sql("select tolower('GOGO')").show()  // error
        val df1: DataFrame = session.read.json("F:\\BigData\\Spark/jsondoc1.txt")
        df1.createTempView("user")

        session.udf.register("userDefinedFunctionexer1",new userDefinedFunctionexer1)

//        df1.show()
        session.sql("select userDefinedFunctionexer1(age) from user").show()
    }
}

// TODO: 自定义聚合函数：
// 1 继承自定义聚合函数
class userDefinedFunctionexer1 extends UserDefinedAggregateFunction  {
    override def inputSchema: StructType = {
        StructType(StructField("inputColumn",LongType)::Nil)
    }

    override def bufferSchema: StructType = {
        StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)
    }


    override def dataType: DataType = LongType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // sum
        buffer(0) = 0L
        //count
        buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row) = {
        if(!input.isNullAt(0)){
            buffer(0) = buffer.getLong(0) + input.getLong(0)
            buffer(1) = buffer.getLong(1) + 1
        }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
        if(!buffer2.isNullAt(0)){
            buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
            buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
        }

    }

    override def evaluate(buffer: Row): Long = {

        buffer.getLong(0) / buffer.getLong(1)
    }

}

// TODO: 自定义求和  -- 聚合函数

class Myavg extends UserDefinedAggregateFunction{
    // 输入数据的类型
    override def inputSchema: StructType = ???
    // 缓冲区的类型
    override def bufferSchema: StructType = ???
    // 输出数据的类型
    override def dataType: DataType = ???
    // 相同的输入是否应该有相同的输出
    override def deterministic: Boolean = ???
    // 对缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = ???
    // 每个分区数据的聚合 (executor中数据的聚合)
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???
    // 分区间数据聚合
        // 不同buffer中数据的聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    }


    override def evaluate(buffer: Row): Any = ???
}