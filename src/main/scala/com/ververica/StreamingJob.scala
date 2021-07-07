package com.ververica

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.Schema

import java.time.LocalDateTime

object StreamingJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val profiles = env.fromElements(
      (0, 1, LocalDateTime.parse("2021-05-01T00:00:00")),
      (0, 1, LocalDateTime.parse("2021-05-01T00:00:00")),
      (1, 1, LocalDateTime.parse("2021-05-06T00:00:00"))
    )
    val pschema =
      Schema.newBuilder()
        .column("_1", "INT NOT NULL")
        .column("_2", "INT")
        .column("_3", "TIMESTAMP(3)")
        .watermark("_3", "_3")
        .primaryKey("_1")
        .build()
    val profileTable = tableEnv
      .fromDataStream(profiles, pschema)
      .as("profileId", "isFraud", "ftimestamp")
    tableEnv.createTemporaryView("profiles", profileTable)
    profileTable.printSchema()

    val transactions = env.fromElements(
      (2, 0, "10.235.82.179", LocalDateTime.parse("2021-05-02T00:00:00"), "10.235.82.179:0"),
      (2, 1, "10.235.82.179", LocalDateTime.parse("2021-05-04T00:00:00"), "10.235.82.179:1")
    )

    val tschema_id_is_key =
      Schema.newBuilder()
        .column("_1", "INT")
        .column("_2", "INT")
        .column("_3", "STRING NOT NULL")
        .column("_4", "TIMESTAMP(3)")
        .column("_5", "STRING NOT NULL")
        .watermark("_4", "_4")
        .primaryKey("_5")
        .build()
    val transactionTableOnId = tableEnv
      .fromDataStream(transactions, tschema_id_is_key)
      .as("sourceProfile", "targetProfile", "ip", "ttimestamp", "id")
    tableEnv.createTemporaryView("transactions_on_id", transactionTableOnId)
    transactionTableOnId.printSchema()

    val tschema_ip_is_key =
      Schema.newBuilder()
        .column("_1", "INT")
        .column("_2", "INT")
        .column("_3", "STRING NOT NULL")
        .column("_4", "TIMESTAMP(3)")
        .column("_5", "STRING NOT NULL")
        .watermark("_4", "_4")
        .primaryKey("_3")
        .build()
    val transactionTableOnIp = tableEnv
      .fromDataStream(transactions, tschema_ip_is_key)
      .as("sourceProfile", "targetProfile", "ip", "ttimestamp", "id")
    tableEnv.createTemporaryView("transactions_on_ip", transactionTableOnIp)
    transactionTableOnIp.printSchema()

    val queries = env.fromElements(
      ("10.235.82.179", LocalDateTime.parse("2021-05-03T00:00:00")),
      ("10.235.82.179", LocalDateTime.parse("2021-05-05T00:00:00")),
      ("10.235.82.179", LocalDateTime.parse("2021-05-07T00:00:00"))
    )
    val qschema =
      Schema.newBuilder()
        .column("_1", "STRING")
        .column("_2", "TIMESTAMP(3)")
        .watermark("_2", "_2")
        .build()
    val queryTable = tableEnv
      .fromDataStream(queries, qschema)
      .as("ip", "qtimestamp")
    tableEnv.createTemporaryView("queries", queryTable)
    queryTable.printSchema()

//    profiles.printToErr()
//    transactions.printToErr()
//    queries.printToErr()

    val prof_fun = tableEnv.from("profiles").createTemporalTableFunction($"ftimestamp", $"profileId")
    tableEnv.registerFunction("prof_fun", prof_fun)

    val trans_fun_id = tableEnv.from("transactions_on_id").createTemporalTableFunction($"ttimestamp", $"id")
    tableEnv.registerFunction("trans_fun_id", trans_fun_id)

    val trans_fun_ip = tableEnv.from("transactions_on_ip").createTemporalTableFunction($"ttimestamp", $"id")
    tableEnv.registerFunction("trans_fun_ip", trans_fun_ip)

    val original_query = """
      SELECT q.qtimestamp, q.ip, sum(p.isFraud)
      FROM queries AS q,
      LATERAL TABLE(trans_fun_id(qtimestamp)) AS t,
      LATERAL TABLE(prof_fun(qtimestamp)) AS p
      WHERE q.ip = t.ip
      AND t.targetProfile = p.profileId
      GROUP BY q.ip, q.qtimestamp
      """

    // Complains that a field STRING NOT NULL id cannot be added to a set that contains a field STRING id

    // Exception in thread "main" java.lang.AssertionError: Cannot add expression of different type to set:
    //      set type is RecordType(VARCHAR(2147483647) CHARACTER SET "UTF-16LE" ip, TIMESTAMP(3) *ROWTIME* qtimestamp, INTEGER sourceProfile, INTEGER targetProfile, VARCHAR(2147483647) CHARACTER SET "UTF-16LE" ip0, TIMESTAMP(3) *ROWTIME* ttimestamp, VARCHAR(2147483647) CHARACTER SET "UTF-16LE" id) NOT NULL
    //      expression type is RecordType(VARCHAR(2147483647) CHARACTER SET "UTF-16LE" ip, TIMESTAMP(3) *ROWTIME* qtimestamp, INTEGER sourceProfile, INTEGER targetProfile, VARCHAR(2147483647) CHARACTER SET "UTF-16LE" ip0, TIMESTAMP(3) *ROWTIME* ttimestamp, VARCHAR(2147483647) CHARACTER SET "UTF-16LE" NOT NULL id) NOT NULL

    val simpler_query_that_still_fails =
      """
      SELECT q.qtimestamp, q.ip
      FROM queries AS q,
      LATERAL TABLE(trans_fun_id(qtimestamp)) AS t
      WHERE q.ip = t.ip
      GROUP BY q.ip, q.qtimestamp
      """

    // ValidationException: Temporal Table Join requires primary key in versioned table, but no primary key can be found.
    val as_of_query_with_primary_key_not_in_join_predicate = """
      SELECT q.qtimestamp, q.ip
      FROM queries q
      JOIN transactions_on_id FOR SYSTEM_TIME AS OF q.qtimestamp AS t ON t.ip = q.ip
      """

    val as_of_query_that_approximates_desired_query = """
      SELECT q.qtimestamp, q.ip, sum(p.isFraud) AS sumofFraud
      FROM queries q
      JOIN transactions_on_ip FOR SYSTEM_TIME AS OF q.qtimestamp AS t ON t.ip = q.ip
      JOIN profiles FOR SYSTEM_TIME AS OF q.qtimestamp AS p ON t.targetProfile = p.profileId
      GROUP BY q.ip, q.qtimestamp
      """

    //    tableEnv.executeSql(simpler_query_that_still_fails).print()
    //    tableEnv.executeSql(as_of_query_with_primary_key_not_in_join_predicate).print()

    tableEnv.executeSql(as_of_query_that_approximates_desired_query).print()
  }
}
