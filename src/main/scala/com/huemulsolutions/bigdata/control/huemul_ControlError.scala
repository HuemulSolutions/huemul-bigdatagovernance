package com.huemulsolutions.bigdata.control

import com.huemulsolutions.bigdata.common.huemul_Library
import com.huemulsolutions.bigdata.datalake.huemul_DataLake

class huemul_ControlError (huemulLib: huemul_Library) extends Serializable  {
  var ControlError_Trace: String = null
  var ControlError_ClassName: String = null
  var ControlError_FileName: String = null
  var ControlError_LineNumber: Integer = null
  var ControlError_MethodName: String = null
  var ControlError_Message: String = null
  var ControlError_IsError: Boolean = false
  
  def IsOK(): Boolean = {
    return !ControlError_IsError
  }
  
  /**
   * GetError(e: Exception, GetClassName: String, dapi_raw: DAPI_RAW )
   * get error from exception and set DAPI_RAW error info
   */
  def GetError(e: Exception, GetClassName: String, dapi_raw: huemul_DataLake ) {
    //Get Error Info
    GetError(e,GetClassName)
    
    //Set Error in DAPI RAW 
    if (dapi_raw != null)
      dapi_raw.Error_isError = true
//    if (dapi_raw.Error_Text == "") {
//      dapi_raw.Error_Text = e.toString()
//    }
  }
  
  /**
   * GetError(e: Exception, GetClassName: String)
   * get error from exception 
   */
  def GetError(e: Exception, GetClassName: String) {
    ControlError_IsError = true
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
    
    
    println("Error Detail DebugMode:")
    
    println(s"ControlError_ClassName: $ControlError_ClassName")
    println(s"ControlError_FileName: $ControlError_FileName")
    println(s"ControlError_LineNumber: $ControlError_LineNumber")
    println(s"ControlError_MethodName: $ControlError_MethodName")
    println(s"ControlError_Message: $ControlError_Message")
    println(s"ControlError_Trace: $ControlError_Trace")
    
    println(s"Detalle")
    println(e)
   
    
  }
}