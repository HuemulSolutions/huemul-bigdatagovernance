package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql._

class huemul_DataQualityResult extends Serializable {
  var isError: Boolean = false
  var Error_Code: Integer = null
  var Description: String = ""
  var dqDF : DataFrame = null
    
  var profilingResult: huemul_Profiling = new huemul_Profiling()
  
  def GetError(e: Exception, DebugMode: Boolean) {
    isError = true
    Description = e.toString()
    
    if (DebugMode){
      println("ERROR DATA QUALITY")
      e.printStackTrace()
    }
  }
  
}