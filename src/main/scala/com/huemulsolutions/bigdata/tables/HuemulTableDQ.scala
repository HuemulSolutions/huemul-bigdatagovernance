package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql.types._

class HuemulTableDQ extends Serializable  {
  val dq_control_id: HuemulColumns = new HuemulColumns (StringType, false, "DQ Id partition (control_id)", false).setHBaseCatalogMapping("dqinfo")
  val dq_dq_id: HuemulColumns = new HuemulColumns (StringType, false, "Id DQ (dq_id in control_dq table)", false).setHBaseCatalogMapping("dqinfo")
  val dq_error_columnname: HuemulColumns = new HuemulColumns (StringType, false, "DQ Column Name ", false).setHBaseCatalogMapping("dqinfo")
  val dq_error_notification: HuemulColumns = new HuemulColumns (StringType, false, "DQ Notification", false).setHBaseCatalogMapping("dqinfo")
  val dq_error_code: HuemulColumns = new HuemulColumns (StringType, false, "DQ Error Code", false).setHBaseCatalogMapping("dqinfo")
  val dq_error_descripcion: HuemulColumns = new HuemulColumns (StringType, false, "DQ User error descripcion", false).setHBaseCatalogMapping("dqinfo")

  
}