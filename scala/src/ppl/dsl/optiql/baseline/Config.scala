package ppl.dsl.optiql.baseline

object Config {

  val flavor: String = System.getProperty("optiql.flavor")
  val tpch_factor: Int = java.lang.Integer.parseInt(System.getProperty("tpch.factor", "0"))
  val tpch_dir: String = System.getProperty("tpch.dir")

}