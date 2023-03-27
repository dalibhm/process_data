package org.example
package datasets

import org.apache.spark.sql.DataFrame

trait Dataset {
  val filename: String
  val df: DataFrame
  val fields: Array[String]
  def UniqueTickers(): DataFrame
}
