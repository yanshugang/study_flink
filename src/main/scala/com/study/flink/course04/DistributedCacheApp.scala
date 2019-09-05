package com.study.flink.course04

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration


object DistributedCacheApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val file_path = "/Users/yanshugang/Desktop/study_flink/test_data/test.txt"

    // step-1: 注册一个本地/HDFS文件
    env.registerCachedFile(file_path, "scala-dc")

    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop", "spark", "flink", "storm")

    data.map(new RichMapFunction[String, String] {

      // step-2: 在open方法中获取到缓存内容
      override def open(parameters: Configuration): Unit = {
        val dc_file = getRuntimeContext.getDistributedCache().getFile("scala-dc")
        val lines = FileUtils.readLines(dc_file)

        import scala.collection.JavaConverters._  // 将java的list装换成scala的list
        for (ele <- lines.asScala) {
          println(ele)
        }
      }

      override def map(value: String): String = {
        value
      }

    }).print()

  }
}
