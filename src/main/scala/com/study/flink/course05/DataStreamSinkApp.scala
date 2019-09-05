package com.study.flink.course05

/**
  * 自定义sink
  */

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object DataStreamSinkApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

  }

}
