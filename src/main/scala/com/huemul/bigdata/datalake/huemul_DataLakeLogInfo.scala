package com.huemul.bigdata.datalake

import org.apache.spark.sql.types._
import org.apache.spark.sql._

class huemul_DataLakeLogInfo extends Serializable {

  var LogDF: DataFrame = null
  var LogSchema: StructType = null
  var DataFirstRow: String = ""
  //var Control_info_rows: Integer = null
  var DataNumRows: Long = -1
  var Log_isRead: java.lang.Boolean = false
  var Log_isInfoRows: java.lang.Boolean = false  
}