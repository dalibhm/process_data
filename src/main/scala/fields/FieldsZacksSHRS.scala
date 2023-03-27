package org.example
package fields

object FieldsZacksSHRS {
  val fields = Seq(
    StandardField("ticker",	"String",	"Ticker or trading symbol"),
    StandardField("m_ticker",	"String",	"Master ticker or trading symbol"),
    StandardField("comp_name",	"String",	"Company name"),
    StandardField("fye",	"Integer",	"Fiscal Year End Month"),
    StandardField("per_type",	"String",	"Q = Quarterly"),
    StandardField("per_end_date",	"Date",	"Period End Date"),
    StandardField("active_ticker_flag",	"String",	"Active or Dead (Y or N)"),
    StandardField("shares_out",	"BigDecimal(34,12)",	"Common Shares Outstanding from the front page of 10K/Q"),
    StandardField("avg_d_shares",	"BigDecimal(34,12)",	"Average Diluted Shares Outstanding used to calculate the EPS figures"),
  )
}
