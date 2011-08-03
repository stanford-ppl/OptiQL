package ppl.dsl.optiql.baseline.benchmarks.tpch.schema

import collection.mutable.ArrayBuffer
import ppl.dsl.optiql.baseline.containers.DataTable

class Part(
  val p_partkey: Int,
  val p_name: String,
  val p_mfgr: String,
  val p_brand: String,
  val p_type: String,
  val p_size: Int,
  val p_container: String,
  val p_retailprice: Float,
  val p_comment:String
)

class PartTable extends DataTable[Part]  {


  def addRecord(fs: Array[String]) {
    assert(fs.size == 9, "Expecting 9 fields, got: " + fs)
    val record = new Part(fs(0),fs(1),fs(2),fs(3),fs(4),fs(5),fs(6),fs(7),fs(8))
    data.append(record)
  }

  def instantiateTable() = new PartTable

}