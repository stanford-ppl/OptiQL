package ppl.dsl.optiql.baseline.benchmarks.tpch.schema

import collection.mutable.ArrayBuffer
import ppl.dsl.optiql.baseline.containers.DataTable


class Customer (
  val c_custkey: Int,
  val c_name: String,
  val c_address: String,
  val c_nationkey: Int,
  val c_phone: String,
  val c_acctbal: Double,
  val c_mktsegment: String,
  val c_comment: String
)

class CustomerTable extends DataTable[Customer]  {


  def addRecord(fs: Array[String]) {
    assert(fs.size == 8, "Expecting 8 fields, got: " + fs.toList.toString)
    val record = new Customer(fs(0),fs(1),fs(2),fs(3),fs(4),fs(5),fs(6),fs(7))
    data.append(record)
  }

  def instantiateTable() = new CustomerTable


}


