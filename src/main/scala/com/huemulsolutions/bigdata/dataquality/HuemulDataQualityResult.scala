package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql._
import scala.collection.mutable._
//import org.apache.spark.sql.types._
//import com.huemulsolutions.bigdata.tables._

class HuemulDataQualityResult extends Serializable {
  var isError: Boolean = false
  var isWarning: Boolean = false
  var errorCode: Integer = _
  var description: String = ""
  var dqDF : DataFrame = _
  var detailErrorsDF: DataFrame = _
  
    
  var profilingResult: HuemulProfiling = new HuemulProfiling()
  private val DQ_Result: ArrayBuffer[HuemulDqRecord] = new ArrayBuffer[HuemulDqRecord]()
  def getDqResult: ArrayBuffer[HuemulDqRecord] = DQ_Result
  
  
  
  def appendDQResult(value: HuemulDqRecord) {
    DQ_Result.append(value)
  }
  def GetError(e: Exception, DebugMode: Boolean) {
    isError = true
    description = e.toString
    
    if (DebugMode){
      println("ERROR DATA QUALITY")
      e.printStackTrace()
    }
  }
  
  
  
}