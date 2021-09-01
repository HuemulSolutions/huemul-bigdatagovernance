package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQNotification.HuemulTypeDQNotification
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQQueryLevel._

class HuemulDQRecord(huemulBigDataGov: HuemulBigDataGovernance) extends Serializable {
  var Table_Name: String = _
  var BBDD_Name: String= _
  var DF_Alias: String= _
  var ColumnName: String= _
  var DQ_Id: String = huemulBigDataGov.huemul_GetUniqueId()
  var DQ_Name: String= _
  var DQ_Description: String= _
  var DQ_QueryLevel: HuemulTypeDQQueryLevel = _
  var DQ_Notification: HuemulTypeDQNotification = _
  var DQ_SQLFormula: String= _
  var DQ_toleranceError_Rows: java.lang.Long = 0
  var DQ_toleranceError_Percent: Decimal= _
  var DQ_ResultDQ: String= _
  var DQ_ErrorCode: Integer = _
  var DQ_ExternalCode: String = _
  var DQ_NumRowsOK: Long = 0
  var DQ_NumRowsError: Long= 0
  var DQ_NumRowsTotal: Long= 0
  var DQ_IsError: Boolean = false
  var DQ_IsWarning: Boolean = false
  var DQ_duration_hour: Integer = _
  var DQ_duration_minute: Integer = _
  var DQ_duration_second: Integer = _
}