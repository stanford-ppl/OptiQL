package ppl.dsl.optiql.baseline.benchmarks.tpch.schema

import ppl.dsl.optiql.baseline.containers.DataTable

class Supplier(
  val s_suppkey: Int,
  val s_name: String,
  val s_address: String,
  val s_nationkey: Int,
  val s_phone: String,
  val s_acctbal: Float,
  val s_comment: String
)

class SupplierTable extends DataTable[Supplier] {


  def addRecord(fs: Array[String]) {
    assert(fs.size == 7, "Expecting 7 fields, got: " + fs)
    val record = new Supplier(fs(0),fs(1),fs(2),fs(3),fs(4),fs(5),fs(6))
    data.append(record)
  }

  def instantiateTable() = new SupplierTable


}
