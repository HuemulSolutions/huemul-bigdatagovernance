package com.huemulsolutions.bigdata.control

import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import com.huemulsolutions.bigdata.datalake.HuemulDataLake

class HuemulControlError(huemulBigDataGov: HuemulBigDataGovernance) extends Serializable  {
  var ControlError_Trace: String = _
  var ControlError_ClassName: String = _
  var ControlError_FileName: String = _
  var ControlError_LineNumber: Integer = _
  var ControlError_MethodName: String = _
  var ControlError_Message: String = _
  var ControlError_IsError: Boolean = false
  var ControlError_ErrorCode: Integer = _
  
  def IsOK(): Boolean = {
    !ControlError_IsError
  }
  
  /**
   * GetError(e: Exception, GetClassName: String, DataLakeInstance: huemul_DataLake )
   * get error from exception and set huemul_DataLake error info
   */
  def GetError(e: Exception, GetClassName: String, DataLakeInstance: HuemulDataLake, Error_Code: Integer ) {
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
    e.getStackTrace.foreach { x => ControlError_Trace = ControlError_Trace + x + "\n" }
    
    e.getStackTrace.foreach { x =>
      if (x.getClassName == GetClassName && ControlError_ClassName == null){
        ControlError_ClassName = x.getClassName
        ControlError_FileName = x.getFileName
        ControlError_LineNumber = x.getLineNumber
        ControlError_MethodName = x.getMethodName
        
      }
    }
    
    if (ControlError_ClassName == null) {
      if (e.getStackTrace == null || e.getStackTrace.length == 0 ) {
        println(e.getStackTrace)
        ControlError_ClassName = "NO INFORMATION AVAILABLE"
        ControlError_FileName = "NO INFORMATION AVAILABLE"
        ControlError_LineNumber = -1
        ControlError_MethodName = "NO INFORMATION AVAILABLE"
      } else if (e.getStackTrace()(0).getClassName == "scala.sys.package$") {
        ControlError_ClassName = e.getStackTrace()(1).getClassName
        ControlError_FileName = e.getStackTrace()(1).getFileName
        ControlError_LineNumber = e.getStackTrace()(1).getLineNumber
        ControlError_MethodName = e.getStackTrace()(1).getMethodName
      } else {
        ControlError_ClassName = e.getStackTrace()(0).getClassName
        ControlError_FileName = e.getStackTrace()(0).getFileName
        ControlError_LineNumber = e.getStackTrace()(0).getLineNumber
        ControlError_MethodName = e.getStackTrace()(0).getMethodName
      }
    }
    
    ControlError_Message = e.toString
    
    huemulBigDataGov.logMessageError("***************************************************************")
    huemulBigDataGov.logMessageError("huemulBigDataGov: Error Detail")
    huemulBigDataGov.logMessageError("***************************************************************")
    huemulBigDataGov.logMessageError(s"ControlError_ClassName: $ControlError_ClassName")
    huemulBigDataGov.logMessageError(s"ControlError_FileName: $ControlError_FileName")
    huemulBigDataGov.logMessageError(s"ControlError_ErrorCode: $ControlError_ErrorCode")
    huemulBigDataGov.logMessageError(s"ControlError_LineNumber: $ControlError_LineNumber")
    huemulBigDataGov.logMessageError(s"ControlError_MethodName: $ControlError_MethodName")
    huemulBigDataGov.logMessageError(s"ControlError_Message: $ControlError_Message")
    huemulBigDataGov.logMessageError(s"ControlError_Trace: $ControlError_Trace")
    
    huemulBigDataGov.logMessageError(s"Detail")
    huemulBigDataGov.logMessageError(e)
   
    
  }
}