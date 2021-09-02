package com.huemulsolutions.bigdata.datalake

import org.apache.spark.sql.types._
import org.apache.spark.sql._

class HuemulDataLakeLogInfo extends Serializable {

  var logDF: DataFrame = _
  var logSchema: StructType = _
  var dataFirstRow: String = ""
  //var Control_info_rows: Integer = null
  var dataNumRows: Long = -1
  var logIsRead: java.lang.Boolean = false
  var logIsInfoRows: java.lang.Boolean = false
}