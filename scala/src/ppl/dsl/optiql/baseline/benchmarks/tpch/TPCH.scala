package ppl.dsl.optiql.baseline.benchmarks.tpch

import schema._
import ppl.dsl.optiql.baseline.containers.DataTable
import java.io._
import ppl.dsl.optiql.baseline.util.{PerformanceTimer, Date, Interval}
import com.sun.corba.se.spi.activation._ActivatorImplBase
import ppl.dsl.optiql.baseline.{Config, OptiQL}


object TPCH {

  val tpchDataPath= Config.tpch_dir + File.separator + "SF" + Config.tpch_factor
  val debug = true

  log("TPCH style benchmarking of OptiQL")

  //actual tables
  val customers = new CustomerTable;
  loadTPCHTable("customer",customers)
  val lineItems = new LineItemTable
  loadTPCHTable("lineitem", lineItems)
  val nations = new NationTable
  loadTPCHTable("nation", nations)
  val orders = new OrderTable
  loadTPCHTable("orders", orders)
  val parts = new PartTable
  loadTPCHTable("part", parts)
  val partSuppliers = new PartSupplierTable
  loadTPCHTable("partsupp", partSuppliers)
  val regions = new RegionTable
  loadTPCHTable("region", regions)
  val suppliers = new SupplierTable
  loadTPCHTable("supplier", suppliers)

  def loadTPCHTable[T](path: String, table: DataTable[T]) {
    log("loading tpch table from file[" + tpchDataPath + "/" + path +"] into memory")
      val filename = tpchDataPath + "/" + path + ".tbl"
      val file = new File(filename)
      if(file.isFile == false) throw new RuntimeException(filename + " doesn't appear to be a valid file")
      //load each line
      val records = scala.io.Source.fromFile(file).getLines()
      var i = 0
      while(records.hasNext) {
        val record = records.next
        val fields = record.split('|')
        table.addRecord(fields)
        i += 1
        if(i%500000 == 0) println("processed " + i + " records")
      }
  }

  def log(msg: String) {
    if(debug) println(msg)
  }



  def main(args: Array[String]) {

   import OptiQL._

    log("Loaded input, now executing Queries")
    //Execute TPC-H queries against my tables
    //val q1 = lineItems Where(_.lineNumber < 5)

    println("TPCH Q1:")
    PerformanceTimer.start("q1",false)
    val q1 = lineItems Where(_.l_shipdate <= Date("1998-12-01") + Interval(90).days) GroupBy(l => (l.l_returnflag,l.l_linestatus)) Select(g => new {
      val returnFlag = g.key._1
      val lineStatus = g.key._2
      val sumQty = g.Sum(_.l_quantity)
      val sumBasePrice = g.Sum(_.l_extendedprice)
      val sumDiscountedPrice = g.Sum(l => l.l_extendedprice * (1-l.l_discount))
      val sumCharge = g.Sum(l=> l.l_extendedprice * (1-l.l_discount) * (1+l.l_tax))
      val avgQty = g.Average(_.l_quantity)
      val avgPrice = g.Average(_.l_extendedprice)
      val avgDiscount = g.Average(_.l_discount)
      val countOrder = g.Count
    }) OrderBy(_.returnFlag) ThenBy(_.lineStatus)
    println(q1.iterator) // to force it to sort
    PerformanceTimer.stop("q1",false)
    PerformanceTimer.print("q1")
    q1.printAsTable()

    println("TPCH Q2:")
    PerformanceTimer.start("q2", false)
    val q2 = parts.Where(_.p_size == 15).Where(_.p_type.endsWith("BRASS")).Join(partSuppliers).WhereEq(_.p_partkey, _.ps_partkey).Select((p, ps) => new {
      val p_partkey = p.p_partkey
      val p_mfgr = p.p_mfgr
      val ps_suppkey = ps.ps_suppkey
      val ps_supplycost = ps.ps_supplycost
    }).Join(suppliers).WhereEq(_.ps_suppkey,_.s_suppkey).Select((j, s) => new {
      val s_acctbal = s.s_acctbal
      val s_name = s.s_name
      val p_partkey = j.p_partkey
      val p_mfgr = j.p_mfgr
      val s_address = s.s_address
      val s_phone = s.s_phone
      val s_comment = s.s_comment
      val ps_supplycost = j.ps_supplycost
      val s_nationkey = s.s_nationkey
    }).Join(nations).WhereEq(_.s_nationkey, _.n_nationkey).Select((j, n) => new {
      val s_acctbal = j.s_acctbal
      val s_name = j.s_name
      val n_name = n.n_name
      val p_partkey = j.p_partkey
      val p_mfgr = j.p_mfgr
      val s_address = j.s_address
      val s_phone = j.s_phone
      val s_comment = j.s_comment
      val ps_supplycost = j.ps_supplycost
      val n_regionkey = n.n_regionkey
    }).Join(regions).WhereEq(_.n_regionkey, _.r_regionkey).Select((j, r) => new {
      val s_acctbal = j.s_acctbal
      val s_name = j.s_name
      val n_name = j.n_name
      val p_partkey = j.p_partkey
      val p_mfgr = j.p_mfgr
      val s_address = j.s_address
      val s_phone = j.s_phone
      val s_comment = j.s_comment
      val ps_supplycost = j.ps_supplycost
      val r_name = r.r_name
    }).Where(_.r_name == "EUROPE").Where(j1 => j1.ps_supplycost == {
      val pssc = partSuppliers.Where(_.ps_partkey == j1.p_partkey).
        Join(suppliers).WhereEq(_.ps_suppkey, _.s_suppkey).Select((ps, s) => new {
        val ps_supplycost = ps.ps_supplycost
        val s_nationkey = s.s_nationkey
       }).Join(nations).WhereEq(_.s_nationkey, _.n_nationkey).Select((jj1, n) => new {
         val ps_supplycost = jj1.ps_supplycost
         val n_regionkey = n.n_regionkey
       }).Join(regions).WhereEq(_.n_regionkey, _.r_regionkey).Select((jj2, r) => new {
         val ps_supplycost = jj2.ps_supplycost
         val r_name = r.r_name
       }).Where(_.r_name == "EUROPE").Min(_.ps_supplycost); if(pssc != null) pssc.ps_supplycost else -10}
    ).OrderByDescending(_.s_acctbal).ThenBy(_.n_name).ThenBy(_.s_name).ThenBy (_.p_partkey)
    println(q2.iterator) // to force it to sort
    PerformanceTimer.stop("q2", false)
    PerformanceTimer.print("q2")
    q2.printAsTable(100)

    println("TPCH Q2:")
    PerformanceTimer.start("q2", false)
    val q2 = regions.Where(_.r_name == "EUROPE").Join(nations).WhereEq(_.r_regionkey, _.n_regionkey).
      With(suppliers).WhereEq((_:Nation).n_nationkey, (_:Supplier).s_nationkey).
      With(partSuppliers).WhereEq((_:Supplier).s_suppkey, (_:PartSupplier).ps_suppkey).
      With(parts).Where(_.p_size == 15).Where(_.p_type.endsWith("BRASS")).WhereEq((_:PartSupplier).ps_partkey, (_:Part).p_partkey).
      Select((r, n, s, ps, p) => new {
        val s_acctbal = s.s_acctbal
        val s_name = s.s_name
        val n_name = n.n_name
        val p_partkey = p.p_partkey
        val p_mfgr = p.p_mfgr
        val s_address = s.s_address
        val s_phone = s.s_phone
        val s_comment = s.s_comment
        val ps_supplycost = ps.ps_supplycost
      }).Where(j => j.ps_supplycost == {
        val pssc = partSuppliers.Where(_.ps_partkey == j.p_partkey).Join(suppliers).WhereEq(_.ps_suppkey, _.s_suppkey).
          With(nations).WhereEq((_:Supplier).s_nationkey, (_:Nation).n_nationkey).
          With(regions).Where(_.r_name == "EUROPE").WhereEq((_:Nation).n_regionkey, (_:Region).r_regionkey).
          Select((ps, s, n, r) => new {
           val ps_supplycost = ps.ps_supplycost
         }).Min(_.ps_supplycost);
        if(pssc != null) pssc.ps_supplycost else -10}
      ).OrderByDescending(_.s_acctbal).ThenBy(_.n_name).ThenBy(_.s_name).ThenBy (_.p_partkey)
    println(q2.iterator) // to force it to sort
    PerformanceTimer.stop("q2", false)
    PerformanceTimer.print("q2")
    q2.printAsTable(100)
//
//    println("TPCH Q3:")
//    PerformanceTimer.start("q3",false)
//    val q3 = customers.Where(_.c_mktsegment == "BUILDING").Join(orders).WhereEq(_.c_custkey, _.o_custkey).
//      With(lineItems).WhereEq((_:Order).o_orderkey, (_:LineItem).l_orderkey).Select((c, o, li)=> new {
//      val orderKey = o.o_orderkey
//      val orderDate = o.o_orderdate
//      val orderShipPriority = o.o_shippriority
//      val orderShipDate = li.l_shipdate
//      val extendedPrice = li.l_extendedprice
//      val discount = li.l_discount
//    }).Where(col => col.orderDate < Date("1995-03-15") && col.orderShipDate > Date("1995-03-15")
//    ).GroupBy(col => (col.orderKey,col.orderDate,col.orderShipPriority)) Select(g => new {
//      val orderKey = g.key._1
//      val revenue = g.Sum(e => e.extendedPrice * (1 - e.discount))
//      val orderDate = g.key._2
//      val shipPriority = g.key._3
//    }) OrderByDescending(_.revenue) ThenBy(_.orderDate)
//    println(q3.iterator)
//    PerformanceTimer.stop("q3",false)
//    q3.printAsTable(10)
//    PerformanceTimer.print("q3")

//    TODO: This takes too long to run, so exclude for now
//    println("TPCH Q4:")
//    PerformanceTimer.start("q4",false)
//    val q4 = orders.Where(o => (o.o_orderdate >= Date("1993-07-01")) &&
//      (o.o_orderdate < (Date("1993-07-01") + Interval(3).months)) &&
//      lineItems.Exists((l:LineItem) => ((l.l_orderkey == o.o_orderkey) && (l.l_commitdate < l.l_receiptdate)))
//    ).GroupBy(_.o_orderpriority).Select(g => new {
//      val o_orderpriority = g.key
//      val order_count = g.Count
//    }) OrderBy(_.o_orderpriority)
//    println(q4.iterator) // to force it to sort
//    PerformanceTimer.stop("q4", false)
//    PerformanceTimer.print("q4")
//    q4.printAsTable(100)


//    TODO: Cannot do Q5 yet because it specifies multiple conditions on which to join (more than the number of joins)
//    println("TPCH Q5:")
//    PerformanceTimer.start("q5",false)
//    val q5 = customers.Join(orders).WhereEq(_.c_custkey, _.o_custkey).
//      With(lineItems).WhereEq((_:Order).o_orderkey, (_:LineItem).l_orderkey).
//      With(suppliers).WhereEq((_:LineItem).l_suppkey, (_:Supplier).s_suppkey).
//
//    println(q4.iterator) // to force it to sort
//    PerformanceTimer.stop("q5", false)
//    PerformanceTimer.print("q5")
//    q5.printAsTable(100)

    println("TPCH Q6:")
    PerformanceTimer.start("q6",false)
    val q6 = lineItems.Where(_.l_shipdate >= Date("1994-01-01")).
      Where(_.l_shipdate < Date("1995-01-01")).
      Where(l => l.l_discount <= 0.07 && l.l_discount >= 0.05).
      Where(_.l_quantity < 24).
      Sum(l => l.l_extendedprice * l.l_discount)
    PerformanceTimer.stop("q6", false)
    PerformanceTimer.print("q6")
    print("Revenue : " + q6)

    // Q7-22, we will get back to it

  }



}