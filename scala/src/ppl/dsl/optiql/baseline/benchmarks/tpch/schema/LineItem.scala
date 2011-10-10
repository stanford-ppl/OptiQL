package ppl.dsl.optiql.baseline.benchmarks.tpch.schema

import ppl.dsl.optiql.baseline.util.Date
import collection.mutable.ArrayBuffer
import ppl.dsl.optiql.baseline.containers.DataTable


class LineItem (
  val l_orderkey: Int,
  val l_partkey: Int,
  val l_suppkey: Int,
  val l_linenumber: Int,
  val l_quantity: Double,
  val l_extendedprice: Double,
  val l_discount: Double,
  val l_tax: Double,
  val l_returnflag: Char,
  val l_linestatus: Char,
  val l_shipdate: Date,
  val l_commitdate: Date,
  val l_receiptdate: Date,
  val l_shipinstruct: String,
  val l_shipmode: String,
  val l_comment: String
)

class LineItemTable extends DataTable[LineItem] {



  def addRecord(fs: Array[String]) {
    assert(fs.size == 16, "Expecting 16 fields, got: " + fs)
    val record = new LineItem(fs(0),fs(1),fs(2),fs(3),fs(4),fs(5),fs(6),fs(7),fs(8),fs(9),fs(10),fs(11),fs(12),fs(13),fs(14),fs(15))
    data.append(record)
  }

  def instantiateTable() = new LineItemTable


}