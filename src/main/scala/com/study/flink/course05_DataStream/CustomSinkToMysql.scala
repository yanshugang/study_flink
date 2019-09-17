package com.study.flink.course05_DataStream

/**
  * 自定义sink
  */

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

case class Student(name: String, id: Int, age: Int)

class CustomSinkToMysql extends RichSinkFunction[Student] {

  override def open(parameters: Configuration): Unit = ???

  override def clone(): AnyRef = ???

  // 每条记录执行一次
  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = ???

}
