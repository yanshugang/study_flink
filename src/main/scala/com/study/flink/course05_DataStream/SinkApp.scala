package com.study.flink.course05_DataStream

/**
  * 自定义sink
  */

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object SinkApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

  }

}
