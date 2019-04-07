package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql.types._

class huemul_TableDQ extends Serializable  {
  val _control_id = new huemul_Columns (StringType, false, "DQ Id partition (control_id)", false)
  val _error_columnname = new huemul_Columns (StringType, false, "DQ Column Name ", false)
  val _error_notification = new huemul_Columns (StringType, false, "DQ Notification", false)
  val _error_code = new huemul_Columns (StringType, false, "DQ Error Code", false)
  val _error_descripcion = new huemul_Columns (StringType, false, "DQ User error descripcion", false)

  
}