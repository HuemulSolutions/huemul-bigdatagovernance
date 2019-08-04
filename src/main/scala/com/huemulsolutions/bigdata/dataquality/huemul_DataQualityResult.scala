package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql._
import scala.collection.mutable._
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.tables._

class huemul_DataQualityResult extends Serializable {
  var isError: Boolean = false
  var isWarning: Boolean = false
  var Error_Code: Integer = null
  var Description: String = ""
  var dqDF : DataFrame = null
  var DetailErrorsDF: DataFrame = null
  
    
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