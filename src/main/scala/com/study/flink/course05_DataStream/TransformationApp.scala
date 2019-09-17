package com.study.flink.course05_DataStream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * DataStream Transformation
  */
object TransformationApp {

  def filtetr_function(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)

    data.map(x => {
      println("received: " + x)
      x
    }).filter(_%2 == 0).print()
  }


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    filtetr_function(env)

    env.execute("DataStreamTransformationApp")
  }

}
