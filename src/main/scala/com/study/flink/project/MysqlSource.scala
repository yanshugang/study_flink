package com.study.flink.project


import java.sql.{DriverManager, PreparedStatement}

import akka.stream.impl.fusing.GraphInterpreter.Connection
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

class MysqlSource extends RichParallelSourceFunction[mutable.HashMap[String, String]] {

  var connection: Connection = null
  var ps: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/flink"
    val user = "root"
    val password = "root"

    Class.forName(driver)
    val connection = DriverManager.getConnection(url, user, password) // TODO：
  }

  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {

  }

  override def cancel(): Unit = ???
}
