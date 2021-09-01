package com.huemulsolutions.bigdata.datalake

import org.apache.spark.sql.types._
import org.apache.spark.sql._

class HuemulDataLakeLogInfo extends Serializable {

  var LogDF: DataFrame = _
  var LogSchema: StructType = _
  var DataFirstRow: String = ""
  //var Control_info_rows: Integer = null
  var DataNumRows: Long = -1
  var Log_isRead: java.lang.Boolean = false
  var Log_isInfoRows: java.lang.Boolean = false  
}