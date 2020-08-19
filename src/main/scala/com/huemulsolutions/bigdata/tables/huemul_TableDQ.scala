package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql.types._

class huemul_TableDQ extends Serializable  {
  val dq_control_id: huemul_Columns = new huemul_Columns (StringType, false, "DQ Id partition (control_id)", false).setHBaseCatalogMapping("dqinfo")
  val dq_dq_id: huemul_Columns = new huemul_Columns (StringType, false, "Id DQ (dq_id in control_dq table)", false).setHBaseCatalogMapping("dqinfo")
  val dq_error_columnname: huemul_Columns = new huemul_Columns (StringType, false, "DQ Column Name ", false).setHBaseCatalogMapping("dqinfo")
  val dq_error_notification: huemul_Columns = new huemul_Columns (StringType, false, "DQ Notification", false).setHBaseCatalogMapping("dqinfo")
  val dq_error_code: huemul_Columns = new huemul_Columns (StringType, false, "DQ Error Code", false).setHBaseCatalogMapping("dqinfo")
  val dq_error_descripcion: huemul_Columns = new huemul_Columns (StringType, false, "DQ User error descripcion", false).setHBaseCatalogMapping("dqinfo")

  
}