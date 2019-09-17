package com.study.flink.course04_DataSet

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

object SinkApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data = 1.to(10)
    val text = env.fromCollection(data)

    val file_path = "./test_data/output/t"
    text.writeAsText(file_path, WriteMode.OVERWRITE).setParallelism(3)
    env.execute("tt")

  }
}
