package com.huemulsolutions.bigdata.control

import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import com.huemulsolutions.bigdata.datalake.HuemulDataLake

class HuemulControlError(huemulBigDataGov: HuemulBigDataGovernance) extends Serializable  {
  var controlErrorTrace: String = _
  var controlErrorClassName: String = _
  var controlErrorFileName: String = _
  var controlErrorLineNumber: Integer = _
  var controlErrorMethodName: String = _
  var controlErrorMessage: String = _
  var controlErrorIsError: Boolean = false
  var controlErrorErrorCode: Integer = _

  def isOK: Boolean = {
    !controlErrorIsError
  }

  /**
   * setError(e: Exception, GetClassName: String, DataLakeInstance: huemul_DataLake )
   * get error from exception and set huemul_DataLake error info
   */
  def setError(e: Exception, getClassName: String, dataLakeInstance: HuemulDataLake, errorCode: Integer ) {
    //Get Error Info
    setError(e,getClassName, errorCode)

    if (dataLakeInstance != null)
      dataLakeInstance.errorIsError = true

  }

  /**
   * setError(e: Exception, GetClassName: String)
   * set error from exception
   */
  def setError(e: Exception, GetClassName: String, Error_Code: Integer) {
    controlErrorIsError = true
    controlErrorErrorCode = if (Error_Code == null) controlErrorErrorCode else Error_Code
    controlErrorTrace = ""
    e.getStackTrace.foreach { x => controlErrorTrace = controlErrorTrace + x + "\n" }

    e.getStackTrace.foreach { x =>
      if (x.getClassName == GetClassName && controlErrorClassName == null){
        controlErrorClassName = x.getClassName
        controlErrorFileName = x.getFileName
        controlErrorLineNumber = x.getLineNumber
        controlErrorMethodName = x.getMethodName

      }
    }

    if (controlErrorClassName == null) {
      if (e.getStackTrace == null || e.getStackTrace.length == 0 ) {
        println(e.getStackTrace.mkString("Array(", ", ", ")"))
        controlErrorClassName = "NO INFORMATION AVAILABLE"
        controlErrorFileName = "NO INFORMATION AVAILABLE"
        controlErrorLineNumber = -1
        controlErrorMethodName = "NO INFORMATION AVAILABLE"
      } else if (e.getStackTrace()(0).getClassName == "scala.sys.package$") {
        controlErrorClassName = e.getStackTrace()(1).getClassName
        controlErrorFileName = e.getStackTrace()(1).getFileName
        controlErrorLineNumber = e.getStackTrace()(1).getLineNumber
        controlErrorMethodName = e.getStackTrace()(1).getMethodName
      } else {
        controlErrorClassName = e.getStackTrace()(0).getClassName
        controlErrorFileName = e.getStackTrace()(0).getFileName
        controlErrorLineNumber = e.getStackTrace()(0).getLineNumber
        controlErrorMethodName = e.getStackTrace()(0).getMethodName
      }
    }

    controlErrorMessage = e.toString

    huemulBigDataGov.logMessageError("***************************************************************")
    huemulBigDataGov.logMessageError("huemulBigDataGov: Error Detail")
    huemulBigDataGov.logMessageError("***************************************************************")
    huemulBigDataGov.logMessageError(s"ControlError_ClassName: $controlErrorClassName")
    huemulBigDataGov.logMessageError(s"ControlError_FileName: $controlErrorFileName")
    huemulBigDataGov.logMessageError(s"ControlError_ErrorCode: $controlErrorErrorCode")
    huemulBigDataGov.logMessageError(s"ControlError_LineNumber: $controlErrorLineNumber")
    huemulBigDataGov.logMessageError(s"ControlError_MethodName: $controlErrorMethodName")
    huemulBigDataGov.logMessageError(s"ControlError_Message: $controlErrorMessage")
    huemulBigDataGov.logMessageError(s"ControlError_Trace: $controlErrorTrace")
    
    huemulBigDataGov.logMessageError(s"Detail")
    huemulBigDataGov.logMessageError(e)
   
    
  }
}