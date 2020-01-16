package com.atguigu.sparkproject01.bean

/**
 * @author gogo
 * @create 2020-01-11 10:12
 */
case class SessionInfo(sessionId:String,count:Long) extends Ordered[SessionInfo]{
    // todo if else 结构： 设置结果1，-1。这样避免直接相减出现如果值相等结果为零，集合会认为两对象相等，只会保留一个。设置成正负可解决问题。
    override def compare(that: SessionInfo): Int = if(this.count <= that.count) 1 else -1
}
