package com.study.flink.course02

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * flink实时处理demo：词频统计
  */
object DemoStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取数据
    val text = env.socketTextStream("localhost", 9999)

    // 隐式转换
    import org.apache.flink.api.scala._
    
    text.flatMap(_.split(",")).map((_, 1)).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1)

    // 执行
    env.execute("DemoStream")


  }

}
