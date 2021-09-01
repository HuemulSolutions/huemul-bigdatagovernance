package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql._
import scala.collection.mutable._
//import org.apache.spark.sql.types._
//import com.huemulsolutions.bigdata.tables._

class HuemulDataQualityResult extends Serializable {
  var isError: Boolean = false
  var isWarning: Boolean = false
  var Error_Code: Integer = _
  var Description: String = ""
  var dqDF : DataFrame = _
  var DetailErrorsDF: DataFrame = _
  
    
  var profilingResult: HuemulProfiling = new HuemulProfiling()
  private val DQ_Result: ArrayBuffer[HuemulDQRecord] = new ArrayBuffer[HuemulDQRecord]()
  def getDQResult: ArrayBuffer[HuemulDQRecord] = DQ_Result
  
  
  
  def appendDQResult(value: HuemulDQRecord) {
    DQ_Result.append(value)
  }
  def GetError(e: Exception, DebugMode: Boolean) {
    isError = true
    Description = e.toString
    
    if (DebugMode){
      println("ERROR DATA QUALITY")
      e.printStackTrace()
    }
  }
  
  
  
}