package org.example
package fields

object FieldsZacksMKTV {
  val fields = Seq(
    StandardField("ticker",	"String",	"Ticker or trading symbol"),
    StandardField("m_ticker",	"String",	"Master ticker or trading symbol"),
    StandardField("comp_name",	"String",	"Company name"),
    StandardField("fye",	"Integer",	"Fiscal Year End Month"),
    StandardField("per_type",	"String",	"Q = Quarterly"),
    StandardField("per_end_date",	"Date",	"Period End Date"),
    StandardField("active_ticker_flag",	"String",	"Active or Dead (Y or N)"),
    StandardField("mkt_val",	"BigDecimal(34,12)",	"Market Cap (shares out x last monthly price per share)"),
    StandardField("ep_val",	"BigDecimal(34,12)",	"Enterprise Value (Market Cap + Longterm Debt (excluding Mortgages & Conv Debt) + Current Portion of LT Debt + Preferred Stock (Cash + Marketable Securities)"),
  )

}
