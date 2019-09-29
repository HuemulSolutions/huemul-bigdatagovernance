package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql.types._

class huemul_TableDQ extends Serializable  {
  val dq_control_id = new huemul_Columns (StringType, false, "DQ Id partition (control_id)", false)
  val dq_error_id = new huemul_Columns (StringType, false, "Id DQ (dq_id in control_dq table)", false)
  val dq_error_columnname = new huemul_Columns (StringType, false, "DQ Column Name ", false)
  val dq_error_notification = new huemul_Columns (StringType, false, "DQ Notification", false)
  val dq_error_code = new huemul_Columns (StringType, false, "DQ Error Code", false)
  val dq_error_descripcion = new huemul_Columns (StringType, false, "DQ User error descripcion", false)

  
}