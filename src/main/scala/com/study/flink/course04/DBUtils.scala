package com.study.flink.course04

import scala.util.Random

object DBUtils {

  def get_connection() = {

    new Random().nextInt(10) + ""
  }

  def return_connection(connection: String): Unit = {

  }

}
