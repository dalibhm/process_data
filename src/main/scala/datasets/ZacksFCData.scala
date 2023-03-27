package org.example
package datasets

import org.apache.spark.sql.{DataFrame, SparkSession}

class ZacksFCData(override val filename: String)(implicit sparkSession: SparkSession) extends Dataset {
  override val fields: Array[String] = Array(
    "tot_revnu", "gross_profit", "tot_oper_exp", "oper_income", "pre_tax_income", "basic_net_eps", "diluted_net_eps", "ebitda", "ebit",
    "cash_sterm_invst", "rcv_tot", "tot_curr_asset", "tot_asset",
    "acct_pay", "tot_curr_liab",  //"tot_lterm_debt",
    "tot_liab",
    "comm_stock_net",  "tang_stock_holder_equity",
    "net_income_loss", "cash_flow_oper_activity", "cash_flow_invst_activity", "tot_comm_pref_stock_div_paid", "fin_activity_other",
    "wavg_shares_out" )

  override val df = sparkSession.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filename)

  override def UniqueTickers(): DataFrame = {
    df.select(col="ticker").distinct()
  }
}
