package com.study.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration


object DataSetDataSourceApp {


  /**
    * 从集合读取数据
    */
  def from_collection(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()

  }

  /**
    * 从文件读取数据
    */
  def from_file(env: ExecutionEnvironment): Unit = {
    val file_path = "./test_data/test.txt" // 可以是文件路径，也可以是文件夹路径
    env.readTextFile(file_path).print()

  }

  /**
    * 从递归文件夹中读取数据
    */
  def from_recursive_file(env: ExecutionEnvironment): Unit = {
    val file_path = "/Users/yanshugang/Desktop/study_flink/test_data/recu_dir"
    val parameters = new Configuration()
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(file_path).withParameters(parameters).print()
  }

  /**
    * 从压缩文件中读取数据
    */
  def from_compressed_file(env: ExecutionEnvironment): Unit = {
    val file_path = "/Users/yanshugang/Desktop/yk_res/comment/comment_民谣.bz2"
    env.readTextFile(file_path).print()

  }

  /**
    * 从csv读取数据
    */
  def from_csv(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val file_path = "./test_data/test.csv"
    // 忽略首行
    env.readCsvFile[(String, Int, String)](file_path, ignoreFirstLine = true).print()
    // 返回指定列
    env.readCsvFile[(Int, String)](file_path, ignoreFirstLine = true, includedFields = Array(1, 2)).print()
    // 也可以使用case class的形式，指定列
    case class MyCaseClass(name: String, age: Int)
    //env.readCsvFile[MyCaseClass](file_path, ignoreFirstLine = true, includedFields = Array(0, 1)).print()  // TODO: 异常
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    from_collection(env)
    from_file(env)
    from_csv(env)
    from_recursive_file(env)
    from_compressed_file(env)


  }

}
