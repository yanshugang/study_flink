package com.study.flink.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object DataSetTransormationApp {


  /**
    * map
    */
  def map_function(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    // data.map((x:Int) => x+1).print()
    // data.map(x => x + 1).print()
    data.map(_ + 1).print()
  }

  /**
    * filter
    */
  def filter_function(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    data.map(_ + 1).filter(_ > 5).print()
  }


  /**
    * mapPartition:
    */
  def map_partition_function(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val students = new ListBuffer[String]
    for (i <- 1 to 100) {
      students.append("student: " + i)
    }
    val data = env.fromCollection(students).setParallelism(3)

    // data.map(x => {
    //   val connection = DBUtils.get_connection()
    //   println(connection + "...")
    //   DBUtils.return_connection(connection)
    // }).print()

    data.mapPartition(x => {
      val connection = DBUtils.get_connection()
      println(connection + "...")
      DBUtils.return_connection(connection)
      x
    }).print()

  }


  /**
    * first
    */
  def first_function(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"))
    info.append((1, "Spark"))
    info.append((1, "Flink"))
    info.append((2, "Java"))
    info.append((2, "Spring Boot"))
    info.append((3, "Linux"))

    import org.apache.flink.api.scala._
    val data = env.fromCollection(info)

    data.first(2).print() // 取前2个元素
    data.groupBy(0).first(2).print() // 分组后取前2
    data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print() //分组后按第二个元素升序排序，取前2

  }

  def flat_map_function(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    import org.apache.flink.api.scala._
    val data = env.fromCollection(info)
    // data.map(_.split(",")).print()
    data.flatMap(_.split(",")).print()
    data.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print() // 词频统计

  }

  def distinct_function(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    import org.apache.flink.api.scala._
    val data = env.fromCollection(info)

    data.flatMap(_.split(",")).distinct().print()
  }

  def join_function(env: ExecutionEnvironment): Unit = {
    val info_1 = ListBuffer[(Int, String)]()
    info_1.append((1, "张三"))
    info_1.append((2, "李四"))
    info_1.append((3, "王五"))
    info_1.append((4, "赵六"))

    val info_2 = ListBuffer[(Int, String)]()
    info_2.append((1, "北京"))
    info_2.append((2, "上海"))
    info_2.append((3, "成都"))
    info_2.append((5, "杭州"))

    import org.apache.flink.api.scala._
    val data_1 = env.fromCollection(info_1)
    val data_2 = env.fromCollection(info_2)

    data_1.join(data_2).where(0).equalTo(0).apply((first, second) => {
      (first._1, first._2, second._2)
    }).print()
  }

  /**
    * 外连接
    */
  def outer_join_function(env: ExecutionEnvironment): Unit = {
    val info_1 = ListBuffer[(Int, String)]()
    info_1.append((1, "张三"))
    info_1.append((2, "李四"))
    info_1.append((3, "王五"))
    info_1.append((4, "赵六"))

    val info_2 = ListBuffer[(Int, String)]()
    info_2.append((1, "北京"))
    info_2.append((2, "上海"))
    info_2.append((3, "成都"))
    info_2.append((5, "杭州"))

    import org.apache.flink.api.scala._
    val data_1 = env.fromCollection(info_1)
    val data_2 = env.fromCollection(info_2)

    // 左连接
    data_1.leftOuterJoin(data_2).where(0).equalTo(0).apply((first, second) => {
      if (second == null) {
        (first._1, first._2, "-")
      } else {
        (first._1, first._2, second._2)
      }

    }).print()

    // 右连接
    println("-" * 10)
    data_1.rightOuterJoin(data_2).where(0).equalTo(0).apply((first, second) => {
      if (first == null) {
        (second._1, "-", second._2)
      } else {
        (second._1, first._2, second._2)
      }

    }).print()

    // 全连接
    println("-" * 10)
    data_1.fullOuterJoin(data_2).where(0).equalTo(0).apply((first, second) => {

      if (first == null) {
        (second._1, "-", second._2)
      } else if (second == null) {
        (first._1, first._2, "-")
      } else {
        (second._1, first._2, second._2)
      }
    }).print()

  }

  /**
    * 笛卡尔积
    */
  def cross_function(env: ExecutionEnvironment): Unit = {
    val info_1 = new ListBuffer[String]()
    info_1.append("曼城")
    info_1.append("曼联")

    val info_2 = new ListBuffer[String]()
    info_2.append("3")
    info_2.append("0")
    info_2.append("1")

    import org.apache.flink.api.scala._
    val data_1 = env.fromCollection(info_1)
    val data_2 = env.fromCollection(info_2)

    data_1.cross(data_2).print()
  }


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    println("~ ~ ~ ~ ~ ~ ~")
    // map_function(env)

    println("~ ~ ~ ~ ~ ~ ~")
    // filter_function(env)

    println("~ ~ ~ ~ ~ ~ ~")
    // map_partition_function(env)

    println("~ ~ ~ ~ ~ ~ ~")
    // first_function(env)

    println("~ ~ ~ ~ ~ ~ ~")
    // flat_map_function(env)

    println("~ ~ ~ ~ ~ ~ ~")
    // distinct_function(env)

    println("~ ~ ~ ~ ~ ~ ~ ")
    // join_function(env)

    println("~ ~ ~ ~ ~ ~ ~ ")
    // outer_join_function(env)

    println("~ ~ ~ ~ ~ ~ ~")
    cross_function(env)
  }
}
