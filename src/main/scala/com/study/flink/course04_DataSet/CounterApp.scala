package com.study.flink.course04_DataSet

/**
  * flink计数器
  */

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object CounterApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop", "spark", "flink", "storm")

    val info = data.map(new RichMapFunction[String, String]() {

      // step-1: 定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // step-2: 注册计数器
        getRuntimeContext.addAccumulator("ele_counts-scala", counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })

    val file_path = "./sink_scala"
    info.writeAsText(file_path, WriteMode.OVERWRITE).setParallelism(3)

    // step-3: 获取计数器
    val job_result = env.execute("CounterApp")
    val num = job_result.getAccumulatorResult[Long]("ele_counts-scala")

    println(num)

  }
}
