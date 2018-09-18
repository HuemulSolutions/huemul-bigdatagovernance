package com.huemulsolutions.bigdata.control

import com.huemulsolutions.bigdata.common.huemul_BigDataGovernance
import com.huemulsolutions.bigdata.datalake.huemul_DataLake

class huemul_ControlError (huemulBigDataGov: huemul_BigDataGovernance) extends Serializable  {
  var ControlError_Trace: String = null
  var ControlError_ClassName: String = null
  var ControlError_FileName: String = null
  var ControlError_LineNumber: Integer = null
  var ControlError_MethodName: String = null
  var ControlError_Message: String = null
  var ControlError_IsError: Boolean = false
  var ControlError_ErrorCode: Integer = null
  
  def IsOK(): Boolean = {
    return !ControlError_IsError
  }
  
  /**
   * GetError(e: Exception, GetClassName: String, DataLakeInstance: huemul_DataLake )
   * get error from exception and set huemul_DataLake error info
   */
  def GetError(e: Exception, GetClassName: String, DataLakeInstance: huemul_DataLake, Error_Code: Integer ) {
    //Get Error Info
    GetError(e,GetClassName, Error_Code)
    
    
    if (DataLakeInstance != null)
      DataLakeInstance.Error_isError = true

  }
  
  /**
   * GetError(e: Exception, GetClassName: String)
   * get error from exception 
   */
  def GetError(e: Exception, GetClassName: String, Error_Code: Integer) {
    ControlError_IsError = true
    ControlError_ErrorCode = if (Error_Code == null) ControlError_ErrorCode else Error_Code
    ControlError_Trace = ""
    e.getStackTrace().foreach { x => ControlError_Trace = ControlError_Trace + x + "\n" }
    
    e.getStackTrace().foreach { x => 
      if (x.getClassName == GetClassName && ControlError_ClassName == null){
        ControlError_ClassName = x.getClassName
        ControlError_FileName = x.getFileName
        ControlError_LineNumber = x.getLineNumber
        ControlError_MethodName = x.getMethodName
        
      }
    }
    
    if (ControlError_ClassName == null) {
      ControlError_ClassName = e.getStackTrace()(0).getClassName
      ControlError_FileName = e.getStackTrace()(0).getFileName
      ControlError_LineNumber = e.getStackTrace()(0).getLineNumber
      ControlError_MethodName = e.getStackTrace()(0).getMethodName  
    }
    
    ControlError_Message = e.toString()
    
    println("***************************************************************")
    println("huemulBigDataGov: Error Detail")
    println("***************************************************************")
    println(s"ControlError_ClassName: $ControlError_ClassName")
    println(s"ControlError_FileName: $ControlError_FileName")
    println(s"ControlError_ErrorCode: $ControlError_ErrorCode")
    println(s"ControlError_LineNumber: $ControlError_LineNumber")
    println(s"ControlError_MethodName: $ControlError_MethodName")
    println(s"ControlError_Message: $ControlError_Message")
    println(s"ControlError_Trace: $ControlError_Trace")
    
    println(s"Detalle")
    println(e)
   
    
  }
}