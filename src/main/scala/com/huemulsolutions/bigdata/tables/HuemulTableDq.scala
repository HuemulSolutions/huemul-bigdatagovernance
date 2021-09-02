package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql.types._

class HuemulTableDq extends Serializable  {
  val dqControlId: HuemulColumns = new HuemulColumns (StringType, false, "DQ Id partition (control_id)", false).setHBaseCatalogMapping("dqinfo")
  val dqDqId: HuemulColumns = new HuemulColumns (StringType, false, "Id DQ (dq_id in control_dq table)", false).setHBaseCatalogMapping("dqinfo")
  val dqErrorColumnName: HuemulColumns = new HuemulColumns (StringType, false, "DQ Column Name ", false).setHBaseCatalogMapping("dqinfo")
  val dqErrorNotification: HuemulColumns = new HuemulColumns (StringType, false, "DQ Notification", false).setHBaseCatalogMapping("dqinfo")
  val dqErrorCode: HuemulColumns = new HuemulColumns (StringType, false, "DQ Error Code", false).setHBaseCatalogMapping("dqinfo")
  val dqErrorDescription: HuemulColumns = new HuemulColumns (StringType, false, "DQ User error descripcion", false).setHBaseCatalogMapping("dqinfo")

  
}