package com.study.flink.course06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

object TableSqlApp {


  case class SalesLog(transactionId: Int, customerId: String, itemId: String, amountPaid: Double)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val table_env = BatchTableEnvironment.create(env)

    val file_path = "./test_data/sales.csv"
    import org.apache.flink.api.scala._
    val csv = env.readCsvFile[SalesLog](file_path, ignoreFirstLine = true)

    // DataSet转Table
    val sales_table = table_env.fromDataSet(csv)
    // 将table注册成表
    table_env.registerTable("sales", sales_table)

    // sql
    val result_table = table_env.sqlQuery("select * from sales")

    // 输出
    table_env.toDataSet[Row](result_table).print()

  }


}
