package com.study.flink.course04_DataSet

import scala.util.Random

object DBUtils {

  def get_connection() = {

    new Random().nextInt(10) + ""
  }

  def return_connection(connection: String): Unit = {

  }

}
