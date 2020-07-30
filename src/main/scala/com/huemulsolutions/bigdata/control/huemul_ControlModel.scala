package com.huemulsolutions.bigdata.control

import com.huemulsolutions.bigdata.common._

/** Class that represent each table of the control model
 *
 * @note    It is a private class only for the package '''com.huemulsolutions.bigdata'''
 *
 * @author  chrstian.sattler@gmail.com
 * @since   2.[6]
 *
 * @groupname process_control Support methods for control model tables
 * @groupname utility Support utility functions
 * @groupname sql_utility Support SQL utility functions (checks, translate) for correct SQL Statement creation
 * @groupname Ungrouped Support functions for control model
 *
 * @param huemulBigDataGov  huemul main class of the big data governance framework
 *
 */
private[bigdata] class huemul_ControlModel(huemulBigDataGov: huemul_BigDataGovernance, controlVersionFull: Integer) extends Serializable {

  //Constructor
  val _huemulBigDataGov: huemul_BigDataGovernance = huemulBigDataGov

  //Constructor-End

  //-------------------------------------------------------------------------------------------------------------------
  /** Stores the parameters of the Spark environment in the controls table '''control_processexecenv'''
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   process_control
   *
   * @param processExecId         Identification of the spark execution
   * @param envCategoryName       Parameter category type (RunTime, SparkProperties, SystemProperties)
   * @param envName               Parameter name (e.g. spark.executor.cores)
   * @param envValue              Parameter value
   * @param mdmProcessName        Process Invocation Name (class name)
   * @return                      huemul_JDBCResult (empty or error)
   */
  def addControlProcessExecEnvDB(processExecId: String
                                 , envCategoryName: String
                                 , envName: String
                                 , envValue: String
                                 , mdmProcessName: String): huemul_JDBCResult =  {

    _huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(
      s"""
         |insert into control_processexecenv (
         |  processexec_id
         |  ,processexecenvcategory_name
         |  ,processexecenv_name
         |  ,processexecenv_value
         |  ,mdm_fhcreate
         |  ,mdm_processname)
         |values(
         |  ${SQLStringNulls(processExecId, 50)}
         |  ,${SQLStringNulls(envCategoryName, 50)}
         |  ,${SQLStringNulls(envName, 500)}
         |  ,${SQLStringNulls(envValue, 4000)}
         |  ,${SQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
         |  ,${SQLStringNulls(mdmProcessName, 200)}
         |)""".stripMargin)


  }

  /** Registe error in the control model database
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   process_control
   *
   * @param ErrorCode       Error code
   * @param message         Error Message
   * @param trace           Error Trace
   * @param fileName        FileName if existe (scala file name of the class)
   * @param methodName      Method Name that register the error
   * @param className       Class Name that register the error
   * @param lineNumber      line number of the error (if available)
   * @param whoWriteError   Name of the error write
   */
  def registerError(ErrorCode: Integer
                    , message: String
                    , trace: String
                    , fileName: String
                    , methodName: String
                    , className: String
                    , lineNumber: Integer
                    , whoWriteError: String ):Unit ={

    if (_huemulBigDataGov.RegisterInControl) {
      val errorId = _huemulBigDataGov.huemul_GetUniqueId()

      val jdbcResult:huemul_JDBCResult =_huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(
        s"""
              insert into control_error (error_id
                                    ,error_message
                                    ,error_code
                                    ,error_trace
                                    ,error_classname
                                    ,error_filename
                                    ,error_linenumber
                                    ,error_methodname
                                    ,error_detail
                                    ,mdm_fhcrea
                                    ,mdm_processname)
        	VALUES( ${SQLStringNulls(errorId)}
          		  ,${SQLStringNulls(message)}
                ,${SQLStringNulls(ErrorCode)}
          		  ,${SQLStringNulls(trace)}
          		  ,${SQLStringNulls(className)}
          		  ,${SQLStringNulls(fileName)}
          		  ,${SQLStringNulls(lineNumber)}
          		  ,${SQLStringNulls(methodName)}
          		  ,''
          		  ,${SQLStringNulls(_huemulBigDataGov.getCurrentDateTime())}
          		  ,${SQLStringNulls(whoWriteError)}
          )
             """, CallErrorRegister = false)

      if (jdbcResult.IsError) {
        _huemulBigDataGov.logMessageError("**** Error Inserting into control error tabla *****")
        _huemulBigDataGov.logMessageError(jdbcResult.ErrorDescription)
      }
    }
  }

  /** Registe error in the control model database (overloaded method registerError)
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   process_control
   *
   * @param huemulError     [huemul_ControlError] Huemul Control Error class
   * @param whoWriteError   Name of the error write
   */
  def registerError(huemulError: huemul_ControlError, whoWriteError: String):Unit = {
    registerError(huemulError.ControlError_ErrorCode
      , huemulError.ControlError_Message
      , huemulError.ControlError_Trace
      , huemulError.ControlError_FileName
      , huemulError.ControlError_MethodName
      , huemulError.ControlError_ClassName
      , huemulError.ControlError_LineNumber
      , whoWriteError)
  }

  /** NVL utility
   *
   * if dataValue is null, returns nullValue
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   utility
   *
   * ToDo: This should be in a separate object class utility (e.g. huemul_Utility)
   *
   * @param dataValue   Data to be checked for null
   * @param nullValue   Null Value to replace
   * @return            String
   */
  private def nvl(dataValue:String, nullValue:String="null") = if (dataValue==null) nullValue else dataValue


  /** Method the formats text:Any to a valid string for SQL
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   sql_utility
   *
   * @param text  text data to transform
   * @param len   maximun len (optional)
   * @return      String transformed for SQL use
   */
  def SQLStringNulls(text: Any, len: Integer = null): String = {
    if (text==null) return "null"

    text match {
      case s: String =>
        val maxLen:Int = if (len == null || (len !=null && len>3999)) 3999 else len     //Oracle varchar max length restriction
        val result = if (s != null && s.length() > maxLen) s.substring(0,maxLen) else s
        s"'${result.replace("'", "''")}'"
      case x:Any =>
        val data = String.valueOf(x)
        if (!data.matches("^\\d*\\.?\\d*$")) throw new Exception("SQLStringNull: Not a valid datatype for sql string")
        data
    }
  }

}
