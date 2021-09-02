package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification.HuemulTypeDqNotification
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqQueryLevel._

class HuemulDqRecord(huemulBigDataGov: HuemulBigDataGovernance) extends Serializable {
  var tableName: String = _
  var bbddName: String= _
  var dfAlias: String= _
  var columnName: String= _
  var dqId: String = huemulBigDataGov.huemul_GetUniqueId()
  var dqName: String= _
  var dqDescription: String= _
  var dqQueryLevel: HuemulTypeDqQueryLevel = _
  var dqNotification: HuemulTypeDqNotification = _
  var dqSqlFormula: String= _
  var dqToleranceError_Rows: java.lang.Long = 0
  var dqToleranceError_Percent: Decimal= _
  var dqResultDq: String= _
  var dqErrorCode: Integer = _
  var dqExternalCode: String = _
  var dqNumRowsOk: Long = 0
  var dqNumRowsError: Long= 0
  var dqNumRowsTotal: Long= 0
  var dqIsError: Boolean = false
  var dqIsWarning: Boolean = false
  var dqDurationHour: Integer = _
  var dqDurationMinute: Integer = _
  var dqDurationSecond: Integer = _
}