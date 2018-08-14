package com.huemulsolutions.bigdata.common

import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

class huemul_DQRecord extends Serializable {
  var Table_Name: String = null
  var BBDD_Name: String= null
  var DF_Alias: String= null
  var ColumnName: String= null
  var DQ_Name: String= null
  var DQ_Description: String= null
  var DQ_IsAggregate: Boolean= false
  var DQ_RaiseError: Boolean= true
  var DQ_SQLFormula: String= null
  var DQ_Error_MaxNumRows: Long= 0
  var DQ_Error_MaxPercent: Decimal= null
  var DQ_ResultDQ: String= null
  var DQ_NumRowsOK: Long = 0
  var DQ_NumRowsError: Long= 0
  var DQ_NumRowsTotal: Long= 0
}