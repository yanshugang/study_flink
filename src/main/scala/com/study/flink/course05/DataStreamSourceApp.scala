package com.study.flink.course05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object DataStreamSourceApp {


  /**
    * socket-数据源
    */
  def socket_function(env: StreamExecutionEnvironment): Unit = {

    val data = env.socketTextStream("localhost", 9999)
    data.print().setParallelism(2) // 设置并行度为1，即单线程运行
  }

  /**
    * 自定义-非并行-数据源
    */
  def custom_non_parallel_source_function(env: StreamExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print()
  }

  /**
    * 自定义-可并行-数据源
    */
  def custom_parallel_source_function(env: StreamExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(3)
    data.print()
  }


  /**
    *
    */
  def custom_rich_parallel_source_function(env: StreamExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomRichParallelSourceFunction).setParallelism(3)
    data.print()
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    println("~" * 10)
    // socket_function(env)

    println("~" * 10)
    // custom_non_parallel_source_function(env)

    println("~" * 10)
    custom_parallel_source_function(env)

    println("~" * 10)
    custom_rich_parallel_source_function(env)

    env.execute("DataStreamSourceApp")

  }

}
