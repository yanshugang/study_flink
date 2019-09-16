package com.study.flink.course02

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * flink批处理demo：词频统计
  */
object DemoBatch {

  def main(args: Array[String]): Unit = {

    val input_path = ""
    val output_path = ""

    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.readTextFile(input_path)

    // 隐式转换
    import org.apache.flink.api.scala._

    val counts = text.flatMap(_.toLowerCase.split("\t")).filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(output_path, "\n", " ")


  }
}
