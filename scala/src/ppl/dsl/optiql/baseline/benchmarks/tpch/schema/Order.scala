package ppl.dsl.optiql.baseline.benchmarks.tpch.schema

import ppl.dsl.optiql.baseline.util.Date
import collection.mutable.ArrayBuffer
import ppl.dsl.optiql.baseline.containers.DataTable

class Order (
  val o_orderkey: Int,
  val o_custkey: Int,
  val o_orderstatus: Char,
  val o_totalprice: Double,
  val o_orderdate: Date,
  val o_orderpriority: String,
  val o_clerk: String,
  val o_shippriority: Int,
  val o_comment: String
)

class OrderTable extends DataTable[Order] {



  def addRecord(fs: Array[String]) {
    assert(fs.size == 9, "Expecting 9 fields, got: " + fs)
    val record = new Order(fs(0),fs(1),fs(2),fs(3),fs(4),fs(5),fs(6),fs(7),fs(8))
    data.append(record)
  }

  def instantiateTable() = new OrderTable
}