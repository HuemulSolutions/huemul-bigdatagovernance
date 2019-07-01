package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification._

class huemul_DQRecord extends Serializable {
  var Table_Name: String = null
  var BBDD_Name: String= null
  var DF_Alias: String= null
  var ColumnName: String= null
  var DQ_Name: String= null
  var DQ_Description: String= null
  var DQ_QueryLevel: huemulType_DQQueryLevel = null
  var DQ_Notification: huemulType_DQNotification = null
  var DQ_SQLFormula: String= null
  var DQ_toleranceError_Rows: java.lang.Long = 0
  var DQ_toleranceError_Percent: Decimal= null
  var DQ_ResultDQ: String= null
  var DQ_ErrorCode: Integer = null
  var DQ_ExternalCode: String = null
  var DQ_NumRowsOK: Long = 0
  var DQ_NumRowsError: Long= 0
  var DQ_NumRowsTotal: Long= 0
  var DQ_IsError: Boolean = false
}