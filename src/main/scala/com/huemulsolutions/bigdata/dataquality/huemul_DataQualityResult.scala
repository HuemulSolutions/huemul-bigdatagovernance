package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql._
import scala.collection.mutable._
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.tables._

class huemul_DataQualityResult extends Serializable {
  var isError: Boolean = false
  var Error_Code: Integer = null
  var Description: String = ""
  var dqDF : DataFrame = null
  var DetailErrorsDF: DataFrame = null
  
  
  val _control_id = new huemul_Columns (StringType, false, "DQ Id partition (control_id)", false)
  val _error_columnname = new huemul_Columns (StringType, false, "DQ Column Name ", false)
  val _error_notification = new huemul_Columns (StringType, false, "DQ Notification", false)
  val _error_code = new huemul_Columns (StringType, false, "DQ Error Code", false)
  val _error_descripcion = new huemul_Columns (StringType, false, "DQ User error descripcion", false)
  
  
  
    
  var profilingResult: huemul_Profiling = new huemul_Profiling()
  private var DQ_Result: ArrayBuffer[huemul_DQRecord] = new ArrayBuffer[huemul_DQRecord]()
  def getDQResult(): ArrayBuffer[huemul_DQRecord] = {return DQ_Result} 
  
  
  def appendDQResult(value: huemul_DQRecord) {
    DQ_Result.append(value)
  }
  def GetError(e: Exception, DebugMode: Boolean) {
    isError = true
    Description = e.toString()
    
    if (DebugMode){
      println("ERROR DATA QUALITY")
      e.printStackTrace()
    }
  }
  
}