package ppl.dsl.optiql.baseline.benchmarks.tpch.schema
import collection.mutable.ArrayBuffer
import ppl.dsl.optiql.baseline.containers.DataTable


class PartSupplier (
  val ps_partkey: Int,
  val ps_suppkey: Int,
  val ps_availqty: Int,
  val ps_supplycost: Double,
  val ps_comment: String
)

class PartSupplierTable extends DataTable[PartSupplier] {



  def addRecord(fs: Array[String]) {
    assert(fs.size == 5, "Expecting 5 fields, got: " + fs)
    val record = new PartSupplier(fs(0),fs(1),fs(2),fs(3),fs(4))
    data.append(record)
  }

  def instantiateTable() = new PartSupplierTable

}