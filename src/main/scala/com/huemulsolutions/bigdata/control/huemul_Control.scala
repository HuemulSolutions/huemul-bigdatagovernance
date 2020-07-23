package com.huemulsolutions.bigdata.control

/**
 * en vez de getAs se usa esto: .GetValue("table_autoIncUpdate".toLowerCase(),ExecResult_TableId.ResultSet(0)).toString.toInt
 */

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.util.Calendar

import com.huemulsolutions.bigdata.datalake.huemul_DataLake
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel._
import huemulType_Frequency._

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.parsing.json.JSON
import scala.collection.mutable.ArrayBuffer


class huemul_Control (phuemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control, runFrequency: huemulType_Frequency, IsSingleton: Boolean = true, RegisterInControlLog: Boolean = true) extends Serializable  {
  val huemulBigDataGov: huemul_BigDataGovernance = phuemulBigDataGov
  
  private var _version_mayor: Int = 0
  private var _version_minor: Int = 0
  private var _version_patch: Int = 0
  
  val Control_Id: String = huemulBigDataGov.huemul_GetUniqueId() 
  
  val Invoker: Array[StackTraceElement] = new Exception().getStackTrace
  
  val Control_IdParent: String = if (ControlParent == null) null else ControlParent.Control_Id
  val Control_ClassName: String = Invoker(1).getClassName.replace("$", "")
  val Control_ProcessName: String = Invoker(1).getMethodName.replace("$", "")
  val Control_FileName: String = Invoker(1).getFileName.replace("$", "")
  
  var Control_Start_dt: Calendar = Calendar.getInstance()
  var Control_Stop_dt: Calendar = _
  val Control_Error: huemul_ControlError = new huemul_ControlError(huemulBigDataGov)
  val Control_Params: scala.collection.mutable.ListBuffer[huemul_LibraryParams] = new scala.collection.mutable.ListBuffer[huemul_LibraryParams]()
  
  private var LocalIdStep: String = ""
  def getStepId: String = LocalIdStep
  //private var Step_IsDQ: Boolean = false
  private var AdditionalParamsInfo: String = ""
  
  private var processExec_param_others: String = ""
  private var processExec_dtStart: java.util.Calendar = huemulBigDataGov.getCurrentDateTimeJava()
  private var processExec_dtEnd: java.util.Calendar = _
  private var processExecStep_dtStart: java.util.Calendar = _
  private var _testPlanGroup_Id: String = _
  
  private val testPlanDetails: scala.collection.mutable.ListBuffer[huemul_TestPlan] = new scala.collection.mutable.ListBuffer[huemul_TestPlan]() 
  def getTestPlanDetails: scala.collection.mutable.ListBuffer[huemul_TestPlan] =  testPlanDetails

  private val control_QueryArray: ArrayBuffer[huemul_control_query] = new ArrayBuffer[huemul_control_query]()
  private val control_QueryColArray: ArrayBuffer[huemul_control_querycol] = new ArrayBuffer[huemul_control_querycol]()
  private val control_WARNING_EXCLUDE_DQ_Id: ArrayBuffer[String] = new ArrayBuffer[String]()
  
  
  //Find process name in control_process  
  if (RegisterInControlLog && huemulBigDataGov.RegisterInControl) {
    //new from 2.1: get version from control_config
    control_SearchtVersion()
  
    control_process_addOrUpd(Control_ClassName
                  ,Control_ClassName
                  ,Control_FileName
                  ,""
                  ,runFrequency.toString
                  ,""
                  ,Control_ClassName)     
  }
  
  
  //Insert processExcec
  phuemulBigDataGov.logMessageInfo(s"processName: $Control_ClassName, ProcessExec_Id: $Control_Id")
  if (RegisterInControlLog && huemulBigDataGov.RegisterInControl) { 
    control_processExec_add(Control_Id
           ,Control_IdParent
           ,Control_ClassName
           ,huemulBigDataGov.Malla_Id
           ,huemulBigDataGov.IdApplication
           ,huemulBigDataGov.Malla_Id
           ,huemulBigDataGov.DebugMode
           ,huemulBigDataGov.Environment

           ,this.getparamYear() 
           ,this.getparamMonth()
           ,this.getparamDay()
           ,this.getparamHour()
           ,this.getparamMin()
           ,this.getparamSec()

           ,Control_ClassName
           )
          }


  //Stores Spark Session Environment parameter
  saveSparkEnvParams()

  //Insert new record
  if (RegisterInControlLog)
  NewStep("Start")
  
  //*****************************************
  //Start Singleton
  //*****************************************  
  if (IsSingleton && huemulBigDataGov.RegisterInControl) {
    NewStep("SET SINGLETON MODE")
    var NumCycle: Int = 0
    var ContinueInLoop: Boolean = true
    
    var ApplicationInUse: String = null
    while (ContinueInLoop) {
      //Try to add Singleton Mode
      val Ejec = control_singleton_Add(Control_ClassName, huemulBigDataGov.IdApplication, Control_ClassName)
      
      if (Ejec.ResultSet == null)
        ApplicationInUse= null
      else if (Ejec.ResultSet.length == 1)
        ApplicationInUse = Ejec.ResultSet(0).getAs[String]("application_id".toLowerCase())        
      
      //if don't have error, exit
      if (!Ejec.IsError && ApplicationInUse == null)
        ContinueInLoop = false
      else {      
        phuemulBigDataGov.logMessageDebug(s"waiting for Singleton... (class: $Control_ClassName, appId: ${huemulBigDataGov.IdApplication} )")
        //if has error, verify other process still alive
        if (NumCycle == 1) //First cicle don't wait
          Thread.sleep(10000)
        
        NumCycle = 1
        // Obtiene procesos pendientes
        
        huemulBigDataGov.application_StillAlive(ApplicationInUse)
      }              
    }
  }
  
  //*****************************************
  //END Start Singleton
  //*****************************************  

 private var paramYear: Integer = 0
 def getparamYear(): Integer = paramYear
 
 private var paramMonth: Integer = 0
 def getparamMonth(): Integer =  paramMonth
 
 private var paramDay: Integer = 0
 def getparamDay(): Integer =  paramDay
 
 private var paramHour: Integer = 0
 def getparamHour(): Integer =  paramHour
 
 private var paramMin: Integer = 0
 def getparamMin(): Integer =  paramMin
 
 private var paramSec: Integer = 0
 def getparamSec(): Integer =  paramSec
 
 def AddParamYear(name: String, value: Integer) {
   paramYear = value
	 AddParamInformationIntern(name, value.toString)
	 UpdateProcessExecParam("year",value)
 }
 
 def AddParamMonth(name: String, value: Integer) {
   paramMonth = value
	 AddParamInformationIntern(name, value.toString)
	 UpdateProcessExecParam("month",value)
 }
 
 def AddParamDay(name: String, value: Integer) {
   paramDay = value
	 AddParamInformationIntern(name, value.toString)
	 UpdateProcessExecParam("day",value)
 }
 
 def AddParamHour(name: String, value: Integer) {
   paramHour = value
	 AddParamInformationIntern(name, value.toString)
	 UpdateProcessExecParam("hour",value)
 }
 
 def AddParamMin(name: String, value: Integer) {
   paramMin = value
	 AddParamInformationIntern(name, value.toString)
	 UpdateProcessExecParam("min",value)
 }
 
 def AddParamSec(name: String, value: Integer) {
   paramSec = value
	 AddParamInformationIntern(name, value.toString)
	 UpdateProcessExecParam("sec",value)
 }
 
 private def UpdateProcessExecParam (paramName: String, value: Integer) {
    //Insert processExcec
    if (huemulBigDataGov.RegisterInControl) {
      control_processExec_UpdParam(this.Control_Id
                                   ,paramName  
                                   ,value)
    }
 }
  
 
 private def UpdateProcessExecParamInfo (paramName: String, value: String) {
    AdditionalParamsInfo = s"$AdditionalParamsInfo${if (AdditionalParamsInfo == "") "" else ", "} {$paramName}=$value"
    
    //Insert processExcec
    if (huemulBigDataGov.RegisterInControl) {
      control_processExec_UpdParamInfo(this.Control_Id, paramName, value)
      
    }
 }
 
 private def AddParamInformationIntern(name: String, value: String) {
    val NewParam = new huemul_LibraryParams()
    NewParam.param_name = name
    NewParam.param_value = value
    NewParam.param_type = "function"
    Control_Params += NewParam
          
    
    
    //Insert processExcec
    if (huemulBigDataGov.RegisterInControl) {
      control_ProcessExecParams_add(this.Control_Id
                                   ,NewParam.param_name
                                   ,NewParam.param_value
                                   ,Control_ClassName
                         )
    }

    if (huemulBigDataGov.DebugMode){
      phuemulBigDataGov.logMessageDebug(s"Param num: ${Control_Params.length}, name: $name, value: $value")
    }
   
 }
  
 def AddParamInformation(name: String, value: String) {
   //add to param table
   AddParamInformationIntern(name, value)
   
   //Update in processExec paramAddInfo
   UpdateProcessExecParamInfo(name, value)
  }
    
  def FinishProcessOK {

    processExec_dtEnd = huemulBigDataGov.getCurrentDateTimeJava()
    val DiffDate = huemulBigDataGov.getDateTimeDiff(processExec_dtStart, processExec_dtEnd)
  
    if (!huemulBigDataGov.HasName(Control_IdParent)) phuemulBigDataGov.logMessageInfo(s"FINISH ALL OK")
    phuemulBigDataGov.logMessageInfo(s"FINISH processName: $Control_ClassName, ProcessExec_Id: $Control_Id, Time Elapsed: ${"%02d".format(DiffDate.hour + (DiffDate.days*24))}:${"%02d".format(DiffDate.minute)}:${"%02d".format(DiffDate.second)} ")

    if (huemulBigDataGov.RegisterInControl) {
    
      control_processExec_Finish(Control_Id, this.LocalIdStep, null)
      
                        
      if (this.IsSingleton) {
        control_singleton_remove(Control_ClassName)  
      }
    }
  }
  
  def FinishProcessError() {
    processExec_dtEnd = huemulBigDataGov.getCurrentDateTimeJava()
    val DiffDate = huemulBigDataGov.getDateTimeDiff(processExec_dtStart, processExec_dtEnd)
    
    if (Control_IdParent == null) phuemulBigDataGov.logMessageWarn(s"FINISH ERROR")
    phuemulBigDataGov.logMessageError(s"FINISH processName: $Control_ClassName, ProcessExec_Id: $Control_Id, Time Elapsed: ${"%02d".format(DiffDate.hour + (DiffDate.days*24))}:${"%02d".format(DiffDate.minute)}:${"%02d".format(DiffDate.second)} ")

      
    if (huemulBigDataGov.RegisterInControl) {
    
      val Error_Id = huemulBigDataGov.huemul_GetUniqueId()
      if (Control_Error.ControlError_Message == null)
        Control_Error.ControlError_Message = ""
      if (Control_Error.ControlError_Trace == null)
        Control_Error.ControlError_Trace = ""
      if (Control_Error.ControlError_FileName == null)
        Control_Error.ControlError_FileName = ""
      if (Control_Error.ControlError_MethodName == null)
        Control_Error.ControlError_MethodName = ""
      if (Control_Error.ControlError_ClassName == null)
        Control_Error.ControlError_ClassName = ""
        
      //Insert processExcec
      control_Error_finish( this.Control_Id
                           ,this.LocalIdStep
                           ,Error_Id
                           ,Control_Error.ControlError_Message
                           ,Control_Error.ControlError_ErrorCode
                           ,Control_Error.ControlError_Trace
                           ,Control_Error.ControlError_ClassName
                           ,Control_Error.ControlError_FileName
                           ,Control_Error.ControlError_LineNumber.toString
                           ,Control_Error.ControlError_MethodName
                           ,"" //--as Error_Detail
                           ,Control_ClassName
                           )
                           
              
      if (this.IsSingleton) {
        control_singleton_remove(Control_ClassName)
      }
    }
        
                               
  }
  
  
  def RegisterError(ErrorCode: Integer, Message: String, Trace: String, FileName: String, MethodName: String, ClassName: String, LineNumber: Integer, WhoWriteError: String ) {
    
      
    if (huemulBigDataGov.RegisterInControl) {
      
      val Error_Id = huemulBigDataGov.huemul_GetUniqueId()
      
      var message = Message
      if (message == null)
        message = ""
        
      var trace = Trace
      if (trace == null)
        trace = ""
        
      var fileName = FileName
      if (fileName == null)
        fileName = ""
        
      var methodName = MethodName
      if (methodName == null)
        methodName = ""
        
      var className = ClassName
      if (className == null)
        className = ""
        
      //Insert processExcec
      control_Error_register(Error_Id
                             ,message
                             ,ErrorCode
                             ,trace
                             ,className
                             ,fileName
                             ,LineNumber.toString
                             ,methodName
                             ,"" //--as Error_Detail
                             ,WhoWriteError
                           )
    }
        
                               
  }
  
  
  def NewStep(StepName: String) {
    phuemulBigDataGov.logMessageInfo(s"Step: $StepName")
    
   
    //New Step add
    val PreviousLocalIdStep = LocalIdStep
    LocalIdStep = huemulBigDataGov.huemul_GetUniqueId()
    
    
    if (huemulBigDataGov.RegisterInControl) {
      //Insert processExcec
      control_processExecStep_add(LocalIdStep, PreviousLocalIdStep, this.Control_Id, StepName, Control_ClassName)
    }
  }
  
  def RegisterTestPlanFeature(p_Feature_Id: String
                             ,p_TestPlan_Id: String) {
    if (huemulBigDataGov.RegisterInControl) {
       //Insert processExcec
      control_TestPlanFeature_add(p_Feature_Id
                                 ,p_TestPlan_Id
                                 ,Control_ClassName)
    }
    
    
  }
  
  /**
   * Return TestPlan ID
   */
  def RegisterTestPlan(p_testPlanGroup_Id: String
                        ,p_testPlan_name: String
                        ,p_testPlan_description: String
                        ,p_testPlan_resultExpected: String
                        ,p_testPlan_resultReal: String
                        ,p_testPlan_IsOK: Boolean): String = {
    //Create New Id
    val testPlan_Id = huemulBigDataGov.huemul_GetUniqueId()
    
    //Get TestPlanGroup to use in final check
    _testPlanGroup_Id = p_testPlanGroup_Id
    phuemulBigDataGov.logMessageDebug(s"TestPlan ${if (p_testPlan_IsOK) "OK " else "ERROR " }: testPlan_name: $p_testPlan_name, resultExpected: $p_testPlan_resultExpected, resultReal: $p_testPlan_resultReal ")
    
    testPlanDetails.append(new huemul_TestPlan(testPlan_Id
                                              ,p_testPlanGroup_Id
                                              ,p_testPlan_name
                                              ,p_testPlan_description
                                              ,p_testPlan_resultExpected
                                              ,p_testPlan_resultReal
                                              ,p_testPlan_IsOK))
    
    if (huemulBigDataGov.RegisterInControl) {
       //Insert processExcec
      control_TestPlan_add(testPlan_Id
                         , p_testPlanGroup_Id
                         , this.Control_Id
                         , this.Control_ClassName
                         , p_testPlan_name
                         , p_testPlan_description
                         , p_testPlan_resultExpected
                         , p_testPlan_resultReal
                         , p_testPlan_IsOK
                         , Control_ClassName
                        )
    }
    
    testPlan_Id
  }
  
  /**
   * TestPlan_IsOkById: Determine if Testplan finish OK
   * @author sebas_rod
   * @param TotalProcessExpected: indicate total number of process with all test OK. 
   * Version >= 1.4
   */
  def TestPlan_IsOkById(TestPlanId: String, TotalProcessExpected: Integer ): Boolean = {
    var IsOK: Boolean = false
    
    val ResultTestPlan = control_TestPlanTest_GetOK(TestPlanId)
    
    if (ResultTestPlan.ResultSet.length == 1) {
    
      val TotalProcess = ResultTestPlan.GetValue("cantidad",ResultTestPlan.ResultSet(0)).toString.toInt //ResultTestPlan.ResultSet(0).getAs[Long]("cantidad".toLowerCase()).toInt
      val TotalOK = ResultTestPlan.GetValue("total_ok", ResultTestPlan.ResultSet(0)).toString.toInt //ResultTestPlan.ResultSet(0).getAs[Long]("total_ok".toLowerCase()).toInt
     // if (TotalOK == null)
     //   TotalOK = 0
         
      if (TotalProcess != TotalOK) {
        phuemulBigDataGov.logMessageDebug(s"TestPlan_IsOkById with Error: Total Process: $TotalProcess, Total OK: $TotalOK, Total Error: ${TotalProcess-TotalOK}, Total Process Expected: $TotalProcessExpected")
      } else if (TotalProcess != TotalProcessExpected) {
        phuemulBigDataGov.logMessageDebug(s"TestPlan_IsOkById doesn't have the expected process: Total Process: $TotalProcess, Total OK: $TotalOK, Total Error: ${TotalProcess-TotalOK}, Total Process Expected: $TotalProcessExpected")
      } else
        phuemulBigDataGov.logMessageDebug(s"TestPlan_IsOkById OK: Total Process: $TotalProcess, Total OK: $TotalOK, Total Error: ${TotalProcess-TotalOK}, Total Process Expected: $TotalProcessExpected")
        IsOK = true
    }
    
    IsOK
  }
  
  /**
   * TestPlan_CurrentIsOK: Determine if current class Testplan finish OK. for a specific Id use TestPlan_IsOkById
   * @author sebas_rod
   * @param TotalOkExpected (default null): indicate total number of TestPlan expected ok. Null exptected all OK & > 1 testplan
   * Version >= 1.4
   */
  def TestPlan_CurrentIsOK(TotalOkExpected: Integer = null): Boolean = {
    var IsOK: Boolean = false
    
    val NumOK = getTestPlanDetails.count { x => x.gettestPlan_IsOK()  }
    val NumTotal = getTestPlanDetails.length
    
    if (TotalOkExpected == null) {
      
     
      if (NumTotal <= 0)
        IsOK = false
      else if (NumTotal == NumOK)
        IsOK = true
    
      RegisterTestPlan(_testPlanGroup_Id, "TestPlan_IsOK", "FINAL CHECK", "All test plan OK && TestPlan > 0", s"N° Total ($NumTotal) = N° OK ($NumOK)",IsOK)
      phuemulBigDataGov.logMessageDebug(s"TestPlan FINISH: ${if (IsOK) "OK " else "ERROR " }: N° Total: $NumTotal, N° OK: $NumOK, N° Error: ${NumTotal - NumOK} ")
    } else {
       if (NumOK == TotalOkExpected)
         IsOK = true
         
       
       RegisterTestPlan(_testPlanGroup_Id, "TestPlan_IsOK", "FINAL CHECK", "User Expected OK = TestPlan OK", s"N° Total ($NumTotal) = N° OK Expected ($TotalOkExpected)",IsOK)
       phuemulBigDataGov.logMessageDebug(s"TestPlan FINISH: ${if (IsOK) "OK " else "ERROR " }: N° Total: $NumTotal, N° OK: $NumOK, N° Error: ${NumTotal - NumOK}, N° User Expected OK: $TotalOkExpected ")
    }
    
    
    IsOK
  }
  
  
  def RegisterDQuality (Table_Name: String
                             , BBDD_Name: String
                             , DF_Alias: String
                             , ColumnName: String
                             , DQ_Id: String
                             , DQ_Name: String
                             , DQ_Description: String
                             , DQ_QueryLevel: huemulType_DQQueryLevel //DQ_IsAggregate: Boolean
                             , DQ_Notification: huemulType_DQNotification ////DQ_RaiseError: Boolean
                             , DQ_SQLFormula: String
                             , DQ_toleranceError_Rows: java.lang.Long
                             , DQ_toleranceError_Percent: Decimal
                             , DQ_ResultDQ: String
                             , DQ_ErrorCode: Integer
                             , DQ_ExternalCode: String
                             , DQ_NumRowsOK: java.lang.Long
                             , DQ_NumRowsError: java.lang.Long
                             , DQ_NumRowsTotal: java.lang.Long
                             , DQ_IsError: Boolean
                             , DQ_IsWarning: Boolean
                             , DQ_duration_hour: Integer
                             , DQ_duration_minute: Integer
                             , DQ_duration_second: Integer) {
                
    //Create New Id
    //val DQId = huemulBigDataGov.huemul_GetUniqueId() //version 2.1, issue 60

    if (huemulBigDataGov.RegisterInControl) {
      //Insert processExcec
      control_DQ_add(   DQ_Id //DQId
                         , Table_Name
                         , BBDD_Name
                         , this.Control_ClassName
                         , this.Control_Id
                         , ColumnName
                         , DF_Alias
                         , DQ_Name
                         , DQ_Description
                         , DQ_QueryLevel.toString
                         , DQ_Notification.toString
                         , DQ_SQLFormula
                         , DQ_toleranceError_Rows
                         , DQ_toleranceError_Percent
                         , DQ_ResultDQ
                         , DQ_ErrorCode
                         , DQ_ExternalCode
                         , DQ_NumRowsOK
                         , DQ_NumRowsError
                         , DQ_NumRowsTotal
                         , DQ_IsError
                         , DQ_IsWarning
                         ,DQ_duration_hour
                         ,DQ_duration_minute
                         ,DQ_duration_second
                         , Control_ClassName
                         
                         )
    } else {
      if (DQ_Notification == huemulType_DQNotification.WARNING_EXCLUDE && DQ_IsWarning) {
        control_WARNING_EXCLUDE_DQ_Id.append(DQ_Id)
      }
    }
    
  }
  
  def GetCharRepresentation(value: String): String  = {
     if (value == "\t") "TAB" else value
           
  }
  
  def RegisterRAW_USE(dapi_raw: huemul_DataLake, Alias: String) {        
    dapi_raw.setrawFiles_id(huemulBigDataGov.huemul_GetUniqueId())
    
    if (huemulBigDataGov.RegisterInControl) {
      
      //Insert processExcec
      val ExecResult = control_rawFiles_add(dapi_raw.getrawFiles_id()
                         , dapi_raw.LogicalName
                         , dapi_raw.GroupName.toUpperCase()
                         , dapi_raw.Description
                         , dapi_raw.SettingInUse.getContactName
                         , dapi_raw.getFrequency.toString
                         , Control_ClassName
                         )
     
      //dapi_raw.getrawFiles_id if new, otherwise take Id from DataBase                   
      val LocalRawFiles_id = ExecResult.OpenVar
      
      val columnsSQLDecode: ArrayBuffer[com.huemulsolutions.bigdata.sql_decode.huemul_sql_columns] = new ArrayBuffer[com.huemulsolutions.bigdata.sql_decode.huemul_sql_columns]()
      
      //Insert Config Details
      dapi_raw.SettingByDate.foreach { x => 
        //Insert processExcec
        var RAWFilesDet_id = huemulBigDataGov.huemul_GetUniqueId()
        
        val ExecResultDet = control_RAWFilesDet_add(LocalRawFiles_id
                             ,RAWFilesDet_id
                             ,x.getStartDate
                             ,x.getEndDate
                             ,x.getFileName
                             ,x.getLocalPath
                             ,x.GetPath(x.getGlobalPath)
                             ,x.DataSchemaConf.ColSeparatorType.toString
                             ,GetCharRepresentation(x.DataSchemaConf.ColSeparator)
                             ,""  //rawfilesdet_data_headcolstring
                             ,x.LogSchemaConf.ColSeparatorType.toString
                             ,GetCharRepresentation(x.LogSchemaConf.ColSeparator) 
                             ,""  //rawfilesdet_log_headcolstring
                             ,x.getLogNumRowsColumnName
                             ,x.getContactName
                             ,Control_ClassName
                             )
            
        RAWFilesDet_id = ExecResultDet.OpenVar //if new take RAWFilesDet_id, if update take from DataBase
        
        
        //Add Query Info
      val rawQuery_Id = huemulBigDataGov.huemul_GetUniqueId()
      var InsertInQueryCol: Boolean = false
      if (dapi_raw.SettingInUse.getStartDate == x.getStartDate) {
        InsertInQueryCol = true
        
        val rawDuration = huemulBigDataGov.getDateTimeDiff(dapi_raw.StartRead_dt, dapi_raw.StopRead_dt ) 
        val ExecResultQuery = control_Query_add(rawQuery_Id
                                              , this.LocalIdStep
                                              , this.Control_Id
                                              , LocalRawFiles_id
                                              , RAWFilesDet_id
                                              , null
                                              , dapi_raw.DataFramehuemul.Alias
                                              , dapi_raw.FileName //from
                                              , "" //where
                                              , 0
                                              , 0
                                              , p_query_israw = true
                                              , p_query_isfinaltable = false
                                              , p_query_isquery = false  //isquery
                                              , p_query_isreference = false  //isreference
                                              , dapi_raw.DataFramehuemul.getNumRows()
                                              , dapi_raw.Log.DataNumRows
                                              , rawDuration.hour
                                              , rawDuration.minute
                                              , rawDuration.second
                                              , ""
                                              , Control_ClassName)
      }
                                            
         
        if (x.DataSchemaConf.ColumnsDef != null) {
           var pos: Integer = 0
           x.DataSchemaConf.ColumnsDef.foreach { y =>
             control_RAWFilesDetFields_add(LocalRawFiles_id
                                          ,RAWFilesDet_id
                                          ,y.getcolumnName_TI
                                          ,y.getcolumnName_Business
                                          ,y.getDescription
                                          ,y.getDataType.toString
                                          ,pos
                                          ,y.getPosIni
                                          ,y.getPosFin
                                          ,y.getApplyTrim     
                                          ,y.getConvertToNull   
                                          ,Control_ClassName
                                          )
                                          
                                          
             //add QueryColumns
              if (InsertInQueryCol) {
                control_QueryColumn_add(huemulBigDataGov.huemul_GetUniqueId()
                    , rawQuery_Id
                    , RAWFilesDet_id
                    , null
                    , pos
                    , y.getcolumnName_Business
                    , y.getcolumnName_Business
                    , y.getPosIni
                    , y.getPosFin
                    , 0
                    , Control_ClassName)
                    
                val newSQLCol = new com.huemulsolutions.bigdata.sql_decode.huemul_sql_columns()
                newSQLCol.column_name = y.getcolumnName_Business
                newSQLCol.column_sql = y.getcolumnName_Business
                columnsSQLDecode.append(newSQLCol)
              }
              pos += 1
            }
         }
      }
      
      //Register in __temporary
      huemulBigDataGov.addColumnsAndTablesFromQuery(Alias, columnsSQLDecode)
    
      //Insert control_rawFilesUse
      val rawfilesuse_id = huemulBigDataGov.huemul_GetUniqueId()
      control_rawFilesUse_add(LocalRawFiles_id
                             ,rawfilesuse_id
                             ,this.Control_ClassName
                             ,this.Control_Id
                             ,dapi_raw.SettingInUse.getuse_year
                             ,dapi_raw.SettingInUse.getuse_month
                             ,dapi_raw.SettingInUse.getuse_day 
                             ,dapi_raw.SettingInUse.getuse_hour
                             ,dapi_raw.SettingInUse.getuse_minute
                             ,dapi_raw.SettingInUse.getuse_second
                             ,dapi_raw.SettingInUse.getuse_params
                             ,dapi_raw.FileName
                             ,dapi_raw.FileName
                             ,dapi_raw.DataFramehuemul.getNumRows().toString
                             ,"" //--RAWFiles_HeaderLine
                             ,Control_ClassName
                           )
    }
  }
  
  def RegisterMASTER_USE(DefMaster: huemul_Table) {
    
    //Insert control_TablesUse
    if (huemulBigDataGov.RegisterInControl) {
      DefMaster._tablesUseId = LocalIdStep
      
      control_TablesUse_add(  DefMaster.TableName
                             ,DefMaster.getCurrentDataBase()  
                             ,Control_ClassName
                             ,Control_Id
                             ,DefMaster._tablesUseId
                             ,this.getparamYear()
                             ,this.getparamMonth()
                             ,this.getparamDay()  
                             ,this.getparamHour() 
                             ,this.getparamMin()
                             ,this.getparamSec()
                             ,AdditionalParamsInfo
                             ,p_TableUse_Read = true //--as TableUse_Read
                             ,p_TableUse_Write = false //--as TableUse_Write
                             ,null //DefMaster.NumRows_New()
                             ,null //DefMaster.NumRows_Update()
                             ,null //DefMaster.NumRows_Updatable()
                             ,null //DefMaster.NumRows_NoChange() 
                             ,null //DefMaster.NumRows_Delete()
                             ,null //DefMaster.numRows_Excluded()
                             ,null //DefMaster.NumRows_Total()
                             ,DefMaster.getPartitionValue()
                             ,DefMaster._getBackupPath()
                             ,Control_ClassName
                                  )
     
    }
  }
  
  def RegisterMASTER_UPDATE_isused(DefMaster: huemul_Table) {
    if (huemulBigDataGov.RegisterInControl) {
      //Table
      val ExecResultTable = control_Tables_UpdateAtEnd( DefMaster.getCurrentDataBase()
                                                       ,DefMaster.TableName
                                                      , DefMaster._getTable_dq_isused()
                                                      , DefMaster._getTable_ovt_isused()
                                                      , DefMaster._getMDM_AutoInc()
                                                      , DefMaster.getSQLCreateTableScript() ) 
     
    }
  }

  def RegisterMASTER_CREATE_Basic(DefMaster: huemul_Table) {
    if (huemulBigDataGov.IsTableRegistered(DefMaster.TableName))
      return
      
    if (huemulBigDataGov.DebugMode) phuemulBigDataGov.logMessageDebug(s"   Register Table&Columns in Control")
    var LocalNewTable_id = huemulBigDataGov.huemul_GetUniqueId()

    if (huemulBigDataGov.RegisterInControl) {
      //Table
      val ExecResultTable = control_Tables_addOrUpd(LocalNewTable_id
                             ,DefMaster.getCurrentDataBase()
                             ,DefMaster.TableName
                             ,DefMaster.getDescription
                             ,DefMaster.getBusiness_ResponsibleName
                             ,DefMaster.getIT_ResponsibleName
                             ,DefMaster.getPartitionField
                             ,DefMaster.getTableType.toString
                             ,DefMaster.getStorageType.toString
                             ,DefMaster.getLocalPath
                             ,DefMaster.getPath(DefMaster.getGlobalPaths)
                             ,DefMaster.getFullNameWithPath_DQ()
                             ,DefMaster.getFullNameWithPath_OldValueTrace()
                             ,"" //--as Table_SQLCreate
                             ,DefMaster.getFrequency.toString
                             ,DefMaster.getSaveBackup
                             ,Control_ClassName
                             )
      
      LocalNewTable_id = ExecResultTable.OpenVar
      DefMaster._setAutoIncUpate(ExecResultTable.OpenVar2.toLong)
      DefMaster._setControlTableId(LocalNewTable_id)
      
      //Insert control_Columns
      var i: Integer = 0
      var localDatabaseName = DefMaster.getCurrentDataBase()
      DefMaster.getColumns().foreach { x => 
        val Column_Id = huemulBigDataGov.huemul_GetUniqueId()
    
        control_Columns_addOrUpd( LocalNewTable_id
                                 ,Column_Id
                                 ,i
                                 ,x.getMyName(DefMaster.getStorageType)
                                 ,x.Description
                                 ,null //--as Column_Formula
                                 ,x.DataType.sql
                                 ,x.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), DefMaster.getStorageType).sql
                                 ,p_Column_SensibleData = false //--as Column_SensibleData
                                 ,p_Column_EnableDTLog = x.getMDM_EnableDTLog
                                 ,p_Column_EnableOldValue = x.getMDM_EnableOldValue
                                 ,p_Column_EnableProcessLog = x.getMDM_EnableProcessLog
                                 ,p_Column_EnableOldValueTrace = x.getMDM_EnableOldValue_FullTrace
                                 ,x.getDefaultValue
                                 ,x.getSecurityLevel.toString
                                 ,x.getEncryptedType
                                 ,p_Column_ARCO = x.getARCO_Data
                                 ,p_Column_Nullable = x.getNullable
                                 ,p_Column_IsPK = x.getIsPK
                                 ,p_Column_IsUnique = x.getIsUnique
                                 ,x.getDQ_MinLen
                                 ,x.getDQ_MaxLen
                                 ,x.getDQ_MinDecimalValue
                                 ,x.getDQ_MaxDecimalValue
                                 ,x.getDQ_MinDateTimeValue
                                 ,x.getDQ_MaxDateTimeValue
                                 ,x.getDQ_RegExp
                                 ,x.getBusinessGlossary_Id
                                 ,Control_ClassName
                                 )     
          i += 1
        }
      
      //Insert control_tablesrel_add
      DefMaster.getForeingKey().foreach { x =>       
        val p_tablerel_id = huemulBigDataGov.huemul_GetUniqueId()
        val InstanceTable = x._Class_TableName.asInstanceOf[huemul_Table]
        val localDatabaseName = InstanceTable.getCurrentDataBase()
        
        val ResultExecRel = control_TablesRel_add(p_tablerel_id
                             ,LocalNewTable_id 
                             ,InstanceTable.TableName
                             ,localDatabaseName
                             ,x.MyName 
                             ,x.AllowNull
                             ,Control_ClassName)
        
                      
         val IdRel = ResultExecRel.OpenVar
         val PK_Id = ResultExecRel.OpenVar2
  
         x.Relationship.foreach { y => 
           control_TablesRelCol_add (IdRel
                                     ,PK_Id
                                     ,y.PK.getMyName(InstanceTable.getStorageType)
                                     ,LocalNewTable_id
                                     ,y.FK.getMyName(DefMaster.getStorageType)
                                     ,Control_ClassName)
          }
      }
    }
  }
  
  def RegisterTrace_DECODE(defQuery: com.huemulsolutions.bigdata.sql_decode.huemul_sql_decode_result
                          , Alias: String
                          , NumRows: Long
                          , Duration_Hour: Long
                          , Duration_Minute: Long
                          , Duration_Second: Long
                          , finalTable: huemul_Table
                          , isQuery: Boolean
                          , isReferenced: Boolean) {     
    
    if (huemulBigDataGov.RegisterInControl) {
      //Add subquerys
      defQuery.subquery_result.foreach { x => 
        RegisterTrace_DECODE(x //huemul_sql_decode_result
                            ,x.AliasQuery //Alias
                            ,-1 //NumRows
                            ,0 //Duration_Hour
                            ,0 //Duration_Minute
                            ,0 //Duration_Second
                            ,null //finalTable
                            ,isQuery = true //isQuery
                            ,isReferenced = false //isReferenced
                            )
      }
      
      var isFinalTable: Boolean = false 
      var table_id: String = null
      var ColumnList: List[Row] = null
    
      if (finalTable != null) {
        isFinalTable = true
        table_id = finalTable._getControlTableId()
        
        //Get columns
        val ColumnsInfo = control_columns_getByTable(table_id)
        ColumnList = ColumnsInfo.ResultSet.toList
          //val column_id: String = if (ExecResultTable.IsError || ExecResultTable.ResultSet.length == 0) null else ExecResultTable.ResultSet(0).getAs[String]("column_id".toLowerCase())
      }
      
      val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
      val query_id = huemulBigDataGov.huemul_GetUniqueId()
      //Insert control_TablesUse
      control_Query_add(query_id
                      , this.LocalIdStep // p_processexecstep_id
                      , this.Control_Id // p_processexec_id
                      , null //p_rawfiles_id
                      , null //p_rawFilesDet_id
                      , table_id //p_table_id
                      , Alias //defQuery.AliasQuery // p_query_alias
                      , defQuery.from_sql // p_query_sql_from
                      , defQuery.where_sql // p_query_sql_where
                      , defQuery.NumErrors // p_query_numErrors
                      , defQuery.AutoIncSubQuery // p_query_autoinc
                      , p_query_israw = false //p_query_israw
                      , p_query_isfinaltable = isFinalTable //p_query_isfinaltable
                      , p_query_isquery = isQuery
                      , p_query_isreference = isReferenced
                      , NumRows // p_query_numrows_real
                      , -1 //p_query_numrows_expected
                      , Duration_Hour // p_query_duration
                      , Duration_Minute
                      , Duration_Second
                      , "" //p_error_id
                      , Control_ClassName)
      
      var numPos: Int = 0
      defQuery.columns.foreach { x =>        
        
        var column_id: String = null
        if (table_id != null && ColumnList != null && x.column_name != null) {
          //Get column_id
          val resColumn = ColumnList.filter { x_colByTable => x_colByTable.getAs[String]("column_name").toUpperCase() == x.column_name.toUpperCase()  }
          if (resColumn.nonEmpty) {
            column_id = resColumn(0).getAs[String]("column_id")
          }
        }
        
        val querycol_id = huemulBigDataGov.huemul_GetUniqueId()
        control_QueryColumn_add(querycol_id
                              , query_id
                              , null //p_rawfilesdet_id
                              , column_id //null //p_column_id
                              , numPos // p_querycol_pos
                              , x.column_name // p_querycol_name
                              , x.column_sql // p_querycol_sql
                              , x.sql_pos_start //p_querycol_posstart
                              , x.sql_pos_end //p_querycol_posend
                              , 0
                              , Control_ClassName)
        numPos += 1
        
        //Add dependencies
        x.column_origin.foreach { y => 
          val querycolori_id = huemulBigDataGov.huemul_GetUniqueId()
          
          var query_idori: String = null
          var querycol_idori: String = null
          var table_idori: String = null
          var column_idori: String = null
          var rawfilesdet_idori: String = null
          var rawfilesdetfields_idori: String = null
          if (y.trace_database_name == null || y.trace_database_name == "__temporary") {
            //doesn't have DB, origin is query or rawfiles, search in Control Array
            val QueryResult =  control_QueryArray.filter { z_query => z_query.table_name != null && y.trace_table_name != null &&  z_query.table_name.toUpperCase() == y.trace_table_name.toUpperCase() }
            
            if (QueryResult.nonEmpty) {
              //get last temp query inserted in the array (the newest one)
              val rowQuery = QueryResult(QueryResult.length-1)
              query_idori = rowQuery.query_id
              rawfilesdet_idori = rowQuery.rawFilesDet_Id
              
              if (query_idori != null) 
                querycol_idori = control_querycol_getId(query_idori, y.trace_column_name)

              
              //if not null, is raw
              if (rawfilesdet_idori != null)
                rawfilesdetfields_idori = y.trace_column_name
            }
            
          } else {
            //Id from Table, search in catalog
            table_idori = control_tables_getId(y.trace_database_name, y.trace_table_name)
            column_idori = control_columns_getId(table_idori, y.trace_column_name)
          }
          
          control_QueryColumnOri_add(querycolori_id
              , querycol_id //p_querycol_id
              , table_idori
              , column_idori
              , rawfilesdet_idori
              , rawfilesdetfields_idori
              , query_idori
              , querycol_idori
              , y.trace_database_name //p_querycolori_dbname
              , y.trace_table_name //p_querycolori_tabname
              , y.trace_tableAlias_name // p_querycolori_tabalias
              , y.trace_column_name // p_querycolori_colname
              , p_querycolori_isselect = true //p_querycolori_isselect
              , p_querycolori_iswhere = false //p_querycolori_iswhere
              , p_querycolori_ishaving = false //p_querycolori_ishaving
              , p_querycolori_isorder = false //p_querycolori_isorder
              , Control_ClassName)
          
        }
        
      }
    
      val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
      val dif = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
      //println(s"insert sql decode time: ${dif.hour}:${dif.minute}:${dif.second}")
    }
                        
  }
  
  def RegisterMASTER_CREATE_Use(DefMaster: huemul_Table) {     
      
    if (huemulBigDataGov.RegisterInControl) {
      //Insert control_TablesUse
      DefMaster._tablesUseId = LocalIdStep
      
       control_TablesUse_add(  DefMaster.TableName
                             ,DefMaster.getCurrentDataBase()  
                             ,Control_ClassName
                             ,Control_Id
                             ,DefMaster._tablesUseId
                             ,this.getparamYear()
                             ,this.getparamMonth()
                             ,this.getparamDay()  
                             ,this.getparamHour() 
                             ,this.getparamMin()
                             ,this.getparamSec()
                             ,AdditionalParamsInfo
                             ,p_TableUse_Read = false //--as TableUse_Read
                             ,p_TableUse_Write = true //--as TableUse_Write
                             ,DefMaster.NumRows_New()
                             ,DefMaster.NumRows_Update()
                             ,DefMaster.NumRows_Updatable()
                             ,DefMaster.NumRows_NoChange() 
                             ,DefMaster.NumRows_Delete()
                             ,DefMaster.NumRows_Excluded()
                             ,DefMaster.NumRows_Total()
                             ,DefMaster.getPartitionValue()
                             ,DefMaster._getBackupPath()
                             ,Control_ClassName
                            ) 
    }
                        
  }
  
  
  /*
  def RegisterMASTER_Query(DefMaster: huemul_Table) {
    
    //Insert control_TablesUse
    if (huemulBigDataGov.RegisterInControl) {
      DefMaster._tablesUseId = LocalIdStep
      
      control_TablesUse_add(  DefMaster.TableName
                             ,DefMaster.getCurrentDataBase()  
                             ,Control_ClassName
                             ,Control_Id
                             ,DefMaster._tablesUseId
                             ,this.getparamYear()
                             ,this.getparamMonth()
                             ,this.getparamDay()  
                             ,this.getparamHour() 
                             ,this.getparamMin()
                             ,this.getparamSec()
                             ,AdditionalParamsInfo
                             ,true //--as TableUse_Read
                             ,false //--as TableUse_Write
                             ,DefMaster.NumRows_New()
                             ,DefMaster.NumRows_Update()
                             ,DefMaster.NumRows_Updatable()
                             ,DefMaster.NumRows_NoChange() 
                             ,DefMaster.NumRows_Delete()
                             ,DefMaster.NumRows_Total()
                             ,DefMaster.getPartitionValue
                             ,DefMaster._getBackupPath()
                             ,Control_ClassName
                                  )
     
    }
  }
  
  
  */
  
  
  
  
 
  
  def RaiseError(txt: String) {
    sys.error(txt)
  }
  
  def ReplaceSQLStringNulls(ColumnName: String, len: Integer): String = {
     huemulBigDataGov.ReplaceSQLStringNulls(ColumnName, len)
  }
  
  /**********************************************************************************************
   * PROCEDIMIENTOS PARA INSERTAR EN MODELO DE CONTROL
   */
  
  private def control_TestPlan_add (   p_testPlan_Id: String
                       , p_testPlanGroup_Id: String
                       , p_processExec_id: String
                       , p_process_id: String
                       , p_testPlan_name: String
                       , p_testPlan_description: String
                       , p_testPlan_resultExpected: String
                       , p_testPlan_resultReal: String
                       , p_testPlan_IsOK: Boolean    
                       , p_Executor_Name: String): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      insert into control_testplan ( testplan_id             
                                ,testplangroup_id        
                                ,processexec_id          
                                ,process_id              
                                ,testplan_name           
                                ,testplan_description    
                                ,testplan_resultexpected   
                                ,testplan_resultreal     
                                ,testplan_isok           
                                ,mdm_fhcreate            
                                ,mdm_processname         
                              )
      VALUES( ${ReplaceSQLStringNulls(p_testPlan_Id,null)}
           , ${ReplaceSQLStringNulls(p_testPlanGroup_Id,null)}
           , ${ReplaceSQLStringNulls(p_processExec_id,null)}
           , ${ReplaceSQLStringNulls(p_process_id,null)}
           , ${ReplaceSQLStringNulls(p_testPlan_name,200)}
           , ${ReplaceSQLStringNulls(p_testPlan_description,1000)}
           , ${ReplaceSQLStringNulls(p_testPlan_resultExpected,1000)}
           , ${ReplaceSQLStringNulls(p_testPlan_resultReal,1000)}
           , ${if (p_testPlan_IsOK) "1" else "0"}
           , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
           , ${ReplaceSQLStringNulls(p_Executor_Name,200)}
      )
    """)
           
     ExecResult
                       
  }
  
  
  private def control_TestPlanFeature_add (   p_Feature_Id: String
                       , p_testPlan_Id: String
                       , p_Executor_Name: String): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      insert into control_testplanfeature ( feature_id             
                                ,testplan_id                 
                                ,mdm_fhcreate            
                                ,mdm_processname         
                              )
      VALUES( ${ReplaceSQLStringNulls(p_Feature_Id,null)}
           , ${ReplaceSQLStringNulls(p_testPlan_Id,null)}
           , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
           , ${ReplaceSQLStringNulls(p_Executor_Name,200)}
      )
    """)
           
     ExecResult
  }
  
  /**
   * 
   */
  private def control_TestPlanTest_GetOK ( p_testPlan_Id: String): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(
      s"""
                    select count(1) as cantidad, sum(testplan_isok) as total_ok 
                    from control_testplan 
                    where testplangroup_id = '$p_testPlan_Id'
                    and testplan_name = 'TestPlan_IsOK'
          """)
  
     ExecResult
  }
  
  def huemul_toInt(x: Any): Option[Int] = x match {
    case i: Int => Some(i)
    case _ => None
  }
  
  private def control_process_addOrUpd (p_process_id: String
                       , p_process_name: String
                       , p_process_FileName: String
                       , p_process_description: String
                       , p_process_frequency: String
                       , p_process_owner: String
                       , p_mdm_processname: String): huemul_JDBCResult =  {
    //Verify if record exist
    var ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
                    SELECT mdm_manualchange 
                    FROM control_process 
                    WHERE process_id = ${ReplaceSQLStringNulls(p_process_id,null)}
          """)

    if (ExecResult.ResultSet.length == 1) {
      //Record exist, update
      
      //ExecResult.ResultSet(0).schema.printTreeString()
      
      //val MDM_ManualChange =  ExecResult.ResultSet(0).getAs[Long]("mdm_manualchange".toLowerCase()).asInstanceOf[Int]
      val MDM_ManualChange = ExecResult.GetValue("mdm_manualchange".toLowerCase(), ExecResult.ResultSet(0)).toString.toInt
      if (MDM_ManualChange == 0) {
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
                      UPDATE control_process 
                      SET  process_name			    = ${ReplaceSQLStringNulls(p_process_name,200)}
                         , process_filename     = ${ReplaceSQLStringNulls(p_process_FileName,200)}
                    	   , process_description	= ${ReplaceSQLStringNulls(p_process_description,1000)}
                    	   , process_owner			  = ${ReplaceSQLStringNulls(p_process_owner,200)}
                    	   , process_frequency    = ${ReplaceSQLStringNulls(p_process_frequency,50)}
                    	WHERE process_id = ${ReplaceSQLStringNulls(p_process_id,null)}
            """)
      }
    } else {
      //New record
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
              insert into control_process (process_id
                        							   , area_id
                        							   , process_name
                                         , process_filename
                        							   , process_description
                        							   , process_owner
                        							   , process_frequency
                                         , mdm_manualchange
                        							   , mdm_fhcreate
                        							   , mdm_processname) 	
            	VALUES( ${ReplaceSQLStringNulls(p_process_id,null)}
            		   , '0'
            		   , ${ReplaceSQLStringNulls(p_process_name,200)}
                   , ${ReplaceSQLStringNulls(p_process_FileName,200)}
            		   , ${ReplaceSQLStringNulls(p_process_description,1000)}
            		   , ${ReplaceSQLStringNulls(p_process_owner,200)}
            		   , ${ReplaceSQLStringNulls(p_process_frequency,50)}
            		   , 0
            		   , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
            		   , ${ReplaceSQLStringNulls(p_mdm_processname,200)}
              )            		   
            """)
    }
    
     ExecResult
  }
  
  private def control_singleton_Add (   p_singleton_id: String
                       , p_application_Id: String
                       , p_singleton_name: String): huemul_JDBCResult =  {
    var ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
              SELECT application_id
              FROM control_singleton
              WHERE singleton_id = ${ReplaceSQLStringNulls(p_singleton_id,100)}
    """)
    var application_Id: String = ""
    
    if (ExecResult.ResultSet.length == 0) {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      insert into control_singleton (singleton_id
                								   , application_id
                								   , singleton_name
                								   , mdm_fhcreate) 	
  		VALUES( ${ReplaceSQLStringNulls(p_singleton_id,100)}
  			   , ${ReplaceSQLStringNulls(p_application_Id,100)}
  			   , ${ReplaceSQLStringNulls(p_singleton_name,100)}
  			   , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
      )  			   
      """)
      
      //If error, get application id
      if (ExecResult.IsError) {
        phuemulBigDataGov.logMessageError(s"${ExecResult.ErrorDescription}")
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
              SELECT *
              FROM control_singleton
              WHERE singleton_id = ${ReplaceSQLStringNulls(p_singleton_id,100)}
        """)
        
        if (ExecResult.ResultSet.length > 0)
          application_Id = ExecResult.ResultSet(0).getAs[String]("application_id".toLowerCase())
      }
      
    } else {
      application_Id = ExecResult.ResultSet(0).getAs[String]("application_id".toLowerCase())
    }
    
     ExecResult
  }
  
  private def control_singleton_remove (p_singleton_id: String): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      DELETE FROM control_singleton 
      WHERE singleton_id = ${ReplaceSQLStringNulls(p_singleton_id,100)}
    """)
           
     ExecResult
  }
  
  private def control_executors_remove (p_application_Id: String): huemul_JDBCResult =  {
    val ExecResult1 = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""DELETE FROM control_singleton WHERE application_id = ${ReplaceSQLStringNulls(p_application_Id,100)}""")
    val ExecResult2 = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""DELETE FROM control_executors WHERE application_id = ${ReplaceSQLStringNulls(p_application_Id,100)}""")
           
    ExecResult2
  }
  
  private def control_executors_add (p_application_Id: String
                                    ,p_IdSparkPort: String
                                    ,p_IdPortMonitoring: String
                                    ,p_Executor_Name: String
      ): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      insert into control_executors (application_id
                  							   , idsparkport
                  							   , idportmonitoring
                  							   , executor_dtstart
                  							   , executor_name) 	
    	VALUES( ${ReplaceSQLStringNulls(p_application_Id,100)}
    		   , ${ReplaceSQLStringNulls(p_IdSparkPort,50)}
    		   , ${ReplaceSQLStringNulls(p_IdPortMonitoring,500)}
    		   , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
    		   , ${ReplaceSQLStringNulls(p_Executor_Name,100)}
      )    		   
    """)
           
    ExecResult
  }
  
  
  private def control_processExec_UpdParam (p_processExec_id: String
                                    ,p_paramName: String
                                    ,p_value: Integer
      ): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexec 
	    SET processexec_param_$p_paramName = $p_value
      WHERE processexec_id = ${ReplaceSQLStringNulls(p_processExec_id,null)} 
    	
    """)
           
    ExecResult
  }
  
  private def control_processExec_UpdParamInfo (p_processExec_id: String
                                    ,p_paramName: String
                                    ,p_value: String
      ): huemul_JDBCResult =  {
    
    processExec_param_others = s"$processExec_param_others${if (processExec_param_others == "") "" else ", "}{$p_paramName}=$p_value"
    
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexec 
	    SET processexec_param_others = ${ReplaceSQLStringNulls(processExec_param_others,1000)} 
      WHERE processexec_id = ${ReplaceSQLStringNulls(p_processExec_id,null)} 
    	
    """)
           
    ExecResult
  }
  
  
  private def control_processExec_add ( p_processExec_id: String
                                       ,p_processExec_idParent: String
                                       ,p_process_id: String
                                       ,p_Malla_id: String
											                 ,p_application_Id: String
                                       ,p_processExec_WhosRun: String
                                       ,p_processExec_DebugMode: Boolean
                                       ,p_processExec_Environment: String
                											 ,p_processExec_param_year: Integer 
                											 ,p_processExec_param_month: Integer 
                											 ,p_processExec_param_day: Integer 
                											 ,p_processExec_param_hour: Integer 
                											 ,p_processExec_param_min: Integer 
                											 ,p_processExec_param_sec: Integer 
                                       ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    
    this.processExec_dtStart = huemulBigDataGov.getCurrentDateTimeJava()
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_processexec (
                               processexec_id
                              ,processexec_idparent
                              ,process_id
                              ,malla_id
    						              ,application_id
                              ,processexec_isstart
                              ,processexec_iscancelled
                              ,processexec_isenderror
                              ,processexec_isendok
                              ,processexec_dtstart
                              ,processexec_dtend
                              ,processexec_durhour
                              ,processexec_durmin
                              ,processexec_dursec
                              ,processexec_whosrun
                              ,processexec_debugmode
                              ,processexec_environment
                						  ,processexec_param_year	
                						  ,processexec_param_month	
                						  ,processexec_param_day	
                						  ,processexec_param_hour	
                						  ,processexec_param_min	
                						  ,processexec_param_sec	
                						  ,processexec_param_others
                              ${if (getVersionFull() >= 20100) " ,processexec_huemulversion" else "" /* from 2.1: */}
                              ${if (getVersionFull() >= 20100) " ,processexec_controlversion" else "" /* from 2.1: */}
                              ,error_id
                              ,mdm_fhcreate
                              ,mdm_processname)
         VALUES( ${ReplaceSQLStringNulls(p_processExec_id,null)}
    			  ,${ReplaceSQLStringNulls(p_processExec_idParent,null)}
    			  ,${ReplaceSQLStringNulls(p_process_id,200)}
    			  ,${ReplaceSQLStringNulls(p_Malla_id,50)}
    			  ,${ReplaceSQLStringNulls(p_application_Id,100)}
    			  ,1
    			  ,0
    			  ,0
    			  ,0
    			  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
    			  ,null
    			  ,0
    			  ,0
    			  ,0
    			  ,${ReplaceSQLStringNulls(p_processExec_WhosRun,200)}
    			  ,${if (p_processExec_DebugMode) "1" else "0"}
    			  ,${ReplaceSQLStringNulls(p_processExec_Environment,200)}
    			  ,$p_processExec_param_year
    			  ,$p_processExec_param_month
    			  ,$p_processExec_param_day
    			  ,$p_processExec_param_hour
    			  ,$p_processExec_param_min
    			  ,$p_processExec_param_sec
    			  ,''
            ${if (getVersionFull() >= 20100) s" ,${ReplaceSQLStringNulls(huemulBigDataGov.currentVersion ,50)}" else "" /* from 2.1: */}
            ${if (getVersionFull() >= 20100) s" ,${ReplaceSQLStringNulls(s"${getVersionMayor()}.${getVersionMinor()}.${getVersionPatch()}" ,50)}" else "" /* from 2.1: */}
    			  ,null
    			  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
    			  ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )    			  
    	
    """)
           
    ExecResult
  }
  
  
  private def control_processExec_Finish (p_processExec_id: String
                                    ,p_processExecStep_id: String
                                    ,p_error_id: String
      ): huemul_JDBCResult =  {
    val DiffDateStep = huemulBigDataGov.getDateTimeDiff(processExecStep_dtStart, processExec_dtEnd)
     
    
    var ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexecstep
      SET  processexecstep_status  = ${if (p_error_id == null || p_error_id == "") "'OK'" else "'ERROR'"}
    			,processexecstep_dtend   = ${ReplaceSQLStringNulls(huemulBigDataGov.dateTimeFormat.format(processExec_dtEnd.getTime),null )}
    			,processexecstep_durhour = ${DiffDateStep.hour + (DiffDateStep.days*24)}
    			,processexecstep_durmin  = ${DiffDateStep.minute}
    			,processexecstep_dursec  = ${DiffDateStep.second}
    			,error_id				         = ${ReplaceSQLStringNulls(p_error_id,null)}
	    WHERE processexecstep_id = ${ReplaceSQLStringNulls(p_processExecStep_id,null)}    	
    """)
    
    if (!ExecResult.IsError) {
      val DiffDate = huemulBigDataGov.getDateTimeDiff(processExec_dtStart, processExec_dtEnd)
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexec
        SET  processexec_iscancelled = 0
        	  ,processexec_isenderror = ${if (p_error_id == null || p_error_id == "") "0" else "1"}
        	  ,processexec_isendok	  = ${if (p_error_id == null || p_error_id == "") "1" else "0"}
        	  ,processexec_dtend	    = ${ReplaceSQLStringNulls(huemulBigDataGov.dateTimeFormat.format(processExec_dtEnd.getTime),null )}
        	  ,processexec_durhour	  = ${DiffDate.hour + (DiffDate.days*24)}
        	  ,processexec_durmin	    = ${DiffDate.minute}
        	  ,processexec_dursec	    = ${DiffDate.second}
        	  ,error_id				        = ${ReplaceSQLStringNulls(p_error_id,null)}
        	WHERE processexec_id = ${ReplaceSQLStringNulls(p_processExec_id,null )}  	
      """)
    }
           
    ExecResult
  }
  
  
  private def control_processExecStep_add (p_processExecStep_id: String
                                    ,p_processExecStep_idAnt: String
                                    ,p_processExec_id: String
                                    ,p_processExecStep_Name: String
                                    ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    
    if (huemulBigDataGov.HasName(p_processExecStep_idAnt)) {
      val endDataTime = huemulBigDataGov.getCurrentDateTimeJava()
      val DiffDateStep = huemulBigDataGov.getDateTimeDiff(processExecStep_dtStart, endDataTime)
    
    
      val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexecstep
  		SET processexecstep_status  = 'OK'
         ,processexecstep_dtend   = ${ReplaceSQLStringNulls(huemulBigDataGov.dateTimeFormat.format(endDataTime.getTime),null )}
  		   ,processexecstep_durhour = ${DiffDateStep.hour + (DiffDateStep.days*24)}
  		   ,processexecstep_durmin  = ${DiffDateStep.minute}
  		   ,processexecstep_dursec  = ${DiffDateStep.second}
  		WHERE processexecstep_id = ${ReplaceSQLStringNulls(p_processExecStep_idAnt,null)}	
      """)
    }
    
    processExecStep_dtStart = huemulBigDataGov.getCurrentDateTimeJava()
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      insert into control_processexecstep (  processexecstep_id
                      										  ,processexec_id
                      										  ,processexecstep_name
                      										  ,processexecstep_status
                      										  ,processexecstep_dtstart
                      										  ,processexecstep_dtend
                      										  ,processexecstep_durhour
                      										  ,processexecstep_durmin
                      										  ,processexecstep_dursec
                      										  ,error_id
                      										  ,mdm_fhcreate
                      										  ,mdm_processname)
       VALUES(  ${ReplaceSQLStringNulls(p_processExecStep_id,null)}	
      			  ,${ReplaceSQLStringNulls(p_processExec_id,null)}	
      			  ,${ReplaceSQLStringNulls(p_processExecStep_Name,200)}	
      			  ,'RUNNING'
      			  ,${ReplaceSQLStringNulls(huemulBigDataGov.dateTimeFormat.format(processExecStep_dtStart.getTime),null )}
      			  ,null
      			  ,0
      			  ,0
      			  ,0
      			  ,null
      			  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
      			  ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}	
      )
      """)
           
    ExecResult
  }
  
  private def control_tables_getId(p_BBDD_name: String, p_Table_name: String): String = {
    //Get Table Id
    val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select table_id 
          from control_tables 
          where table_bbddname = ${ReplaceSQLStringNulls(p_BBDD_name,null)} 
          and   table_name     = ${ReplaceSQLStringNulls(p_Table_name,null)}	
      """)
    val LocalTable_Id: String = if (ExecResultTable.IsError || ExecResultTable.ResultSet.length == 0) null else ExecResultTable.ResultSet(0).getAs[String]("table_id".toLowerCase()) 
    
    LocalTable_Id
  }
  
  private def control_columns_getId(p_Table_id: String, p_column_name: String): String = {
    //Get Table Id
    val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select column_id 
          from control_columns 
          where table_id = ${ReplaceSQLStringNulls(p_Table_id,null)} 
          and   column_name     = ${ReplaceSQLStringNulls(p_column_name,null)}	
      """)
    val column_id: String = if (ExecResultTable.IsError || ExecResultTable.ResultSet.length == 0) null else ExecResultTable.ResultSet(0).getAs[String]("column_id".toLowerCase()) 
    
    column_id
  }
  
  private def control_columns_getByTable(p_Table_id: String): huemul_JDBCResult = {
    //Get Table Id
    val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select column_id, column_name 
          from control_columns 
          where table_id = ${ReplaceSQLStringNulls(p_Table_id,null)}	
      """)
        
    ExecResultTable
  }
  
  private def control_querycol_getId(p_query_id: String, p_querycol_name: String): String = {
    //Get Table Id
    val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select querycol_id 
          from control_querycolumn 
          where query_id = ${ReplaceSQLStringNulls(p_query_id,null)} 
          and   querycol_name     = ${ReplaceSQLStringNulls(p_querycol_name,null)}	
      """)
    val querycol_id: String = if (ExecResultTable.IsError || ExecResultTable.ResultSet.length == 0) null else ExecResultTable.ResultSet(0).getAs[String]("querycol_id".toLowerCase()) 
    
    querycol_id
  }
  
  
  private def control_DQ_add (p_DQ_Id: String
                             ,p_Table_name: String
                             ,p_BBDD_name: String
                             ,p_Process_Id: String
                             ,p_ProcessExec_Id: String
                             ,p_Column_Name: String
                             ,p_Dq_AliasDF: String
                             ,p_DQ_Name: String
                             ,p_DQ_Description: String
                             ,p_DQ_QueryLevel: String
                             ,p_DQ_Notification: String
                             ,p_DQ_SQLFormula: String
                             ,p_DQ_toleranceError_Rows: java.lang.Long
                             ,p_DQ_toleranceError_Percent: Decimal
                             ,p_DQ_ResultDQ: String
                             ,p_DQ_ErrorCode: Integer
                             ,p_DQ_ExternalCode: String
                             ,p_DQ_NumRowsOK: java.lang.Long
                             ,p_DQ_NumRowsError: java.lang.Long
                             ,p_DQ_NumRowsTotal: java.lang.Long
                             ,p_DQ_IsError: Boolean
                             ,p_DQ_IsWarning: Boolean
                             ,p_DQ_duration_hour: Integer
                             ,p_DQ_duration_minute: Integer
                             ,p_DQ_duration_second: Integer
                             ,p_MDM_ProcessName: String
                             
      ): huemul_JDBCResult =  {
    
    //Get Table Id
    val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select table_id 
          from control_tables 
          where table_bbddname = ${ReplaceSQLStringNulls(p_BBDD_name,null)} 
          and   table_name     = ${ReplaceSQLStringNulls(p_Table_name,null)}	
      """)
    val LocalTable_Id: String = if (ExecResultTable.IsError || ExecResultTable.ResultSet.length == 0) null else ExecResultTable.ResultSet(0).getAs[String]("table_id".toLowerCase()) 
      
    //Get column Id
    val ExecResultColumn = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select column_id 
          from control_columns 
          where table_id     = ${ReplaceSQLStringNulls(LocalTable_Id,null)} 
          and   column_name  = ${ReplaceSQLStringNulls(p_Column_Name,null)}	
      """)
    val LocalColumn_Id: String = if (ExecResultColumn.IsError || ExecResultColumn.ResultSet.length == 0) null else ExecResultColumn.ResultSet(0).getAs[String]("column_id".toLowerCase()) 
      
    
    
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
         insert into control_dq ( dq_id
                                 ,table_id
                                 ,process_id
                                 ,processexec_id
                                 ,column_id
                                 ,column_name
                                 ,dq_aliasdf
                                 ,dq_name
                                 ,dq_description
                                 ,dq_querylevel
                                 ,dq_notification
                                 ,dq_sqlformula
                                 ,dq_dq_toleranceerror_rows
                                 ,dq_dq_toleranceerror_percent
                                 ,dq_resultdq
                                 ,dq_errorcode
                                 ,dq_externalcode
                                 ,dq_numrowsok
                                 ,dq_numrowserror
                                 ,dq_numrowstotal
                                 ,dq_iserror
                                 ,dq_iswarning
                                 ,dq_duration_hour
                                 ,dq_duration_minute
                                 ,dq_duration_second
                                 ,mdm_fhcreate
                                 ,mdm_processname)
      	VALUES(  ${ReplaceSQLStringNulls(p_DQ_Id,null)}
        			 ,${ReplaceSQLStringNulls(LocalTable_Id,null)}
        			 ,${ReplaceSQLStringNulls(p_Process_Id,null)}
        			 ,${ReplaceSQLStringNulls(p_ProcessExec_Id,null)}
        			 ,${ReplaceSQLStringNulls(LocalColumn_Id,null)}
        			 ,${ReplaceSQLStringNulls(p_Column_Name,200)}
        			 ,${ReplaceSQLStringNulls(p_Dq_AliasDF,200)}
        			 ,${ReplaceSQLStringNulls(p_DQ_Name,200)}
        			 ,${ReplaceSQLStringNulls(p_DQ_Description,1000)}
        			 ,${ReplaceSQLStringNulls(p_DQ_QueryLevel,null)}
        			 ,${ReplaceSQLStringNulls(p_DQ_Notification,null)}
        			 ,${ReplaceSQLStringNulls(p_DQ_SQLFormula,4000)}
        			 ,$p_DQ_toleranceError_Rows
        			 ,$p_DQ_toleranceError_Percent
        			 ,${ReplaceSQLStringNulls(p_DQ_ResultDQ,4000)}
               ,$p_DQ_ErrorCode
               ,${ReplaceSQLStringNulls(p_DQ_ExternalCode,200)}
        			 ,$p_DQ_NumRowsOK
        			 ,$p_DQ_NumRowsError
        			 ,$p_DQ_NumRowsTotal
               ,${if (p_DQ_IsError) "1" else "0"}
               ,${if (p_DQ_IsWarning) "1" else "0"}
               ,$p_DQ_duration_hour
               ,$p_DQ_duration_minute
               ,$p_DQ_duration_second
        			 ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
        			 ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )        			 
      """)
           
    ExecResult
  }
  
  /**
   * Get DQ details with dq_notification = WARNING_EXCLUDE and dq_iswarning = 1
   */
  def control_getDQResultForWarningExclude(): ArrayBuffer[String] = {
    //Get Table Id
    var result: ArrayBuffer[String] = new ArrayBuffer[String]()
    
    if (huemulBigDataGov.RegisterInControl) {
      val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
            select  dq_id
            from control_dq
            where processexec_id = ${ReplaceSQLStringNulls(this.Control_Id,null)}
            AND   dq_notification = ${ReplaceSQLStringNulls("WARNING_EXCLUDE",null)}
            AND   dq_iswarning = 1
        """)
      
        
      ExecResultTable.ResultSet.foreach { x =>  
        result.append(x.getAs[String]("dq_id"))
      }   
    } else
      result = control_WARNING_EXCLUDE_DQ_Id
    
    result
  }
  
  def control_getDQResult(): huemul_JDBCResult = {
    //Get Table Id
    val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select  dq_id
                 ,table_id
                 ,process_id
                 ,processexec_id
                 ,column_id
                 ,column_name
                 ,dq_aliasdf
                 ,dq_name
                 ,dq_description
                 ,dq_querylevel
                 ,dq_notification
                 ,dq_sqlformula
                 ,dq_dq_toleranceerror_rows
                 ,dq_dq_toleranceerror_percent
                 ,dq_resultdq
                 ,dq_errorcode
                 ,dq_externalcode
                 ,dq_numrowsok
                 ,dq_numrowserror
                 ,dq_numrowstotal
                 ,dq_iserror
                 ,dq_iswarning
                 ,dq_duration_hour
                 ,dq_duration_minute
                 ,dq_duration_second
                 ,mdm_fhcreate
                 ,mdm_processname 
          from control_dq
          where processexec_id = ${ReplaceSQLStringNulls(this.Control_Id,null)}	
      """)
    
    ExecResultTable
  }
  
  private def control_rawFiles_add (p_RAWFiles_id: String
                             ,p_RAWFiles_LogicalName: String
                             ,p_RAWFiles_GroupName: String
                             ,p_RAWFiles_Description: String
                             ,p_RAWFiles_Owner: String
                             ,p_RAWFiles_frequency: String
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    
    //Get ExecResultRawFiles Id
    val ExecResultRawFiles = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select mdm_manualchange, rawfiles_id 
          from control_rawfiles 
          where rawfiles_groupname = ${ReplaceSQLStringNulls(p_RAWFiles_GroupName,null)} 
          and   rawfiles_logicalname = ${ReplaceSQLStringNulls(p_RAWFiles_LogicalName,null)} 
      """)
      
    var LocalMDM_ManualChange: Int = -1
    var Localrawfiles_id: String = null
    if (!ExecResultRawFiles.IsError && ExecResultRawFiles.ResultSet.length == 1) {
      LocalMDM_ManualChange = ExecResultRawFiles.GetValue("mdm_manualchange",ExecResultRawFiles.ResultSet(0)).toString.toInt
      Localrawfiles_id = ExecResultRawFiles.ResultSet(0).getAs[String]("rawfiles_id".toLowerCase())
      
    }
    
    var ExecResult: huemul_JDBCResult = null
    if (ExecResultRawFiles.ResultSet.length == 1 && LocalMDM_ManualChange == 0) {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_rawfiles
          SET rawfiles_description	= ${ReplaceSQLStringNulls(p_RAWFiles_Description,1000)}
        		 ,rawfiles_owner		    = ${ReplaceSQLStringNulls(p_RAWFiles_Owner,200)}
        		 ,rawfiles_frequency	  = ${ReplaceSQLStringNulls(p_RAWFiles_frequency,200)}
        	WHERE rawfiles_id = ${ReplaceSQLStringNulls(Localrawfiles_id,null)}
        """)
        
      ExecResult.OpenVar = Localrawfiles_id
    } else if (ExecResultRawFiles.ResultSet.length == 0) {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_rawfiles (rawfiles_id
                      								 ,area_id
                      								 ,rawfiles_logicalname
                      								 ,rawfiles_groupname
                      								 ,rawfiles_description
                      								 ,rawfiles_owner
                      								 ,rawfiles_frequency
                      								 ,mdm_manualchange
                      								 ,mdm_fhcreate
                      								 ,mdm_processname)
        	VALUES( ${ReplaceSQLStringNulls(p_RAWFiles_id,null)}
        			 ,'0'
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_LogicalName,500)}
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_GroupName,500)}
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_Description,1000)}
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_Owner,200)}
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_frequency,200)}
        			 ,0 
        			 ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
        			 ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )        			 
        """)
        
      ExecResult.OpenVar = p_RAWFiles_id
    }
           
    ExecResult
  }
  
  
  private def control_RAWFilesDet_add (p_RAWFiles_Id: String
                             ,p_RAWFilesDet_id: String
                             ,p_RAWFilesDet_StartDate: Calendar
                             ,p_RAWFilesDet_EndDate: Calendar
                             ,p_RAWFilesDet_FileName: String
                             ,p_RAWFilesDet_LocalPath: String
                             ,p_RAWFilesDet_GlobalPath: String
                             ,p_rawfilesdet_data_colseptype: String
                             ,p_rawfilesdet_data_colsep: String
                             ,p_rawfilesdet_data_headcolstring: String
                             ,p_rawfilesdet_log_colseptype: String
                             ,p_rawfilesdet_log_colsep: String
                             ,p_rawfilesdet_log_headcolstring: String
                             ,p_rawfilesdet_log_numrowsfield: String
                             ,p_RAWFilesDet_ContactName: String
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    var ExecResult: huemul_JDBCResult = null
    
    val p_RAWFilesDet_StartDate_string: String = huemulBigDataGov.dateTimeFormat.format(p_RAWFilesDet_StartDate.getTime)
    val p_RAWFilesDet_EndDate_string: String = huemulBigDataGov.dateTimeFormat.format(p_RAWFilesDet_EndDate.getTime)
    //Get ExecResultRawFiles Id
    val ExecResultRawFilesDet = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT rawfilesdet_id 
					FROM control_rawfilesdet 
					WHERE rawfiles_id = ${ReplaceSQLStringNulls(p_RAWFiles_Id,null)}
					AND   rawfilesdet_startdate = ${ReplaceSQLStringNulls(p_RAWFilesDet_StartDate_string,null)}
      """)
      
    var Localrawfilesdet_id: String = null
    if (!ExecResultRawFilesDet.IsError && ExecResultRawFilesDet.ResultSet.length == 1) {
      Localrawfilesdet_id = ExecResultRawFilesDet.ResultSet(0).getAs[String]("rawfilesdet_id".toLowerCase())
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          update control_rawfilesdetfields 
          set mdm_active = 0 
          where rawfilesdet_id = 	${ReplaceSQLStringNulls(Localrawfilesdet_id,null)}
        """)
        
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_rawfilesdet
      		SET rawfilesdet_enddate                   = ${ReplaceSQLStringNulls(p_RAWFilesDet_EndDate_string,null)}						
      		   ,rawfilesdet_filename					        = ${ReplaceSQLStringNulls(p_RAWFilesDet_FileName,1000)}					
      		   ,rawfilesdet_localpath					        = ${ReplaceSQLStringNulls(p_RAWFilesDet_LocalPath,1000)}					
      		   ,rawfilesdet_globalpath					      = ${ReplaceSQLStringNulls(p_RAWFilesDet_GlobalPath,1000)}					
      		   ,rawfilesdet_data_colseptype		  = ${ReplaceSQLStringNulls(p_rawfilesdet_data_colseptype,50)}		
      		   ,rawfilesdet_data_colsep			    = ${ReplaceSQLStringNulls(p_rawfilesdet_data_colsep,50)}			
      		   ,rawfilesdet_data_headcolstring	= ${ReplaceSQLStringNulls(p_rawfilesdet_data_headcolstring,4000)}	
      		   ,rawfilesdet_log_colseptype		  = ${ReplaceSQLStringNulls(p_rawfilesdet_log_colseptype,50)}		
      		   ,rawfilesdet_log_colsep			    = ${ReplaceSQLStringNulls(p_rawfilesdet_log_colsep,50)}			
      		   ,rawfilesdet_log_headcolstring		= ${ReplaceSQLStringNulls(p_rawfilesdet_log_headcolstring,4000)}		
      		   ,rawfilesdet_log_numrowsfield		  = ${ReplaceSQLStringNulls(p_rawfilesdet_log_numrowsfield,200)}		
      		   ,rawfilesdet_contactname					      = ${ReplaceSQLStringNulls(p_RAWFilesDet_ContactName,200)}					
      		WHERE rawfilesdet_id = ${ReplaceSQLStringNulls(Localrawfilesdet_id,null)}
        """)
        
      ExecResult.OpenVar = Localrawfilesdet_id
    } else {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_rawfilesdet ( rawfilesdet_id
                    										   ,rawfiles_id
                    										   ,rawfilesdet_startdate
                    										   ,rawfilesdet_enddate
                    										   ,rawfilesdet_filename
                    										   ,rawfilesdet_localpath
                    										   ,rawfilesdet_globalpath
                    										   ,rawfilesdet_data_colseptype
                    										   ,rawfilesdet_data_colsep
                    										   ,rawfilesdet_data_headcolstring
                    										   ,rawfilesdet_log_colseptype
                    										   ,rawfilesdet_log_colsep
                    										   ,rawfilesdet_log_headcolstring
                    										   ,rawfilesdet_log_numrowsfield
                    										   ,rawfilesdet_contactname
                    										   ,mdm_fhcreate
                    										   ,mdm_processname)
        		VALUES(  ${ReplaceSQLStringNulls(p_RAWFilesDet_id,null)}
          			   ,${ReplaceSQLStringNulls(p_RAWFiles_Id,null)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_StartDate_string,null)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_EndDate_string,null)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_FileName,1000)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_LocalPath,1000)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_GlobalPath,1000)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_data_colseptype,50)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_data_colsep,50)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_data_headcolstring,4000)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_log_colseptype,50)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_log_colsep,50)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_log_headcolstring,4000)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_log_numrowsfield,200)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_ContactName,200)}
          			   ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
          			   ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )          			   
        """)
      ExecResult.OpenVar = p_RAWFilesDet_id
    }
    
    
    ExecResult
  }
  
  
  private def control_RAWFilesDetFields_add (p_RAWFiles_Id: String
                             ,p_RAWFilesDet_id: String
                             ,p_RAWFilesDetFields_ITName: String
                             ,p_RAWFilesDetFields_LogicalName: String
                             ,p_RAWFilesDetFields_description: String
                             ,p_RAWFilesDetFields_DataType: String
                             ,P_RAWFilesDetFields_Position: Integer
                             ,P_RAWFilesDetFields_PosIni: Integer
                             ,P_RAWFilesDetFields_PosFin: Integer
                             ,p_RAWFilesDetFields_ApplyTrim: Boolean
                             ,p_RAWFilesDetFields_ConvertNull: Boolean
                             ,P_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    var ExecResult: huemul_JDBCResult = null
    
     //Get ExecResultRawFiles Id
    val ExecResultRawFilesDetFields = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT rawfilesdetfields_logicalname
					FROM control_rawfilesdetfields 
					WHERE rawfilesdet_id = ${ReplaceSQLStringNulls(p_RAWFilesDet_id,null)}
					AND   rawfilesdetfields_logicalname = ${ReplaceSQLStringNulls(p_RAWFilesDetFields_LogicalName,null)}
      """)
      
    
    if (!ExecResultRawFilesDetFields.IsError && ExecResultRawFilesDetFields.ResultSet.length == 1) {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_rawfilesdetfields
        	SET  mdm_active = 1
        		,rawfilesdetfields_itname		    =  ${ReplaceSQLStringNulls(p_RAWFilesDetFields_ITName,200)}
        		,rawfilesdetfields_description  =  ${ReplaceSQLStringNulls(p_RAWFilesDetFields_description,1000)}
        		,rawfilesdetfields_datatype     =  ${ReplaceSQLStringNulls(p_RAWFilesDetFields_DataType,50)}
        		,rawfilesdetfields_position     =  $P_RAWFilesDetFields_Position
        		,rawfilesdetfields_posini       =  $P_RAWFilesDetFields_PosIni
        		,rawfilesdetfields_posfin       =  $P_RAWFilesDetFields_PosFin
        		,rawfilesdetfields_applytrim    =  ${if (p_RAWFilesDetFields_ApplyTrim) "1" else "0"}
        		,rawfilesdetfields_convertnull  =  ${if (p_RAWFilesDetFields_ConvertNull) "1" else "0"}
        	WHERE rawfilesdet_id = ${ReplaceSQLStringNulls(p_RAWFilesDet_id,null)}
        	AND   rawfilesdetfields_logicalname = ${ReplaceSQLStringNulls(p_RAWFilesDetFields_LogicalName,200)}
        """)
    } else {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_rawfilesdetfields (  rawfilesdet_id
                            											,rawfilesdetfields_itname
                            											,rawfilesdetfields_logicalname
                            											,rawfilesdetfields_description
                            											,rawfilesdetfields_datatype
                            											,rawfilesdetfields_position
                            											,rawfilesdetfields_posini
                            											,rawfilesdetfields_posfin
                                                  ,rawfilesdetfields_applytrim
                                                  ,rawfilesdetfields_convertnull 
                            											,mdm_active
                            											,mdm_fhcreate
                            											,mdm_processname)
        	VALUES( ${ReplaceSQLStringNulls(p_RAWFilesDet_id,50)}
          			,${ReplaceSQLStringNulls(p_RAWFilesDetFields_ITName,200)}
          			,${ReplaceSQLStringNulls(p_RAWFilesDetFields_LogicalName,200)}
          			,${ReplaceSQLStringNulls(p_RAWFilesDetFields_description,1000)}
          			,${ReplaceSQLStringNulls(p_RAWFilesDetFields_DataType,50)}
          			,$P_RAWFilesDetFields_Position
          			,$P_RAWFilesDetFields_PosIni
          			,$P_RAWFilesDetFields_PosFin
          			,${if (p_RAWFilesDetFields_ApplyTrim) "1" else "0"}
          			,${if (p_RAWFilesDetFields_ConvertNull) "1" else "0"} 
          			,1
          			,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
          			,${ReplaceSQLStringNulls(P_MDM_ProcessName,200)}
      )          			
        """)
    }
    
    ExecResult
  }
  
  
  private def control_rawFilesUse_add (p_RAWFiles_Id: String
                             ,p_RAWFilesUse_id: String
                             ,p_Process_Id: String
                             ,p_ProcessExec_Id: String
                             ,p_RAWFilesUse_Year: Integer
                             ,p_RAWFilesUse_Month: Integer
                             ,p_RAWFilesUse_Day: Integer
                             ,p_RAWFilesUse_Hour: Integer
                             ,p_RAWFilesUse_Minute: Integer
                             ,p_RAWFilesUse_Second: Integer
                             ,p_RAWFilesUse_params: String
                             ,p_RAWFiles_fullName: String
                             ,p_RAWFiles_fullPath: String
                             ,p_RAWFiles_numRows: String
                             ,p_RAWFiles_HeaderLine: String
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_rawfilesuse (  rawfilesuse_id
                          									 ,rawfiles_id
                          									 ,process_id
                          									 ,processexec_id
                                             ,rawfilesuse_year
                                             ,rawfilesuse_month
                                             ,rawfilesuse_day
                                             ,rawfilesuse_hour
                                             ,rawfilesuse_minute
                                             ,rawfilesuse_second
                                             ,rawfilesuse_params
                          									 ,rawfiles_fullname
                          									 ,rawfiles_fullpath
                          									 ,rawfiles_numrows
                          									 ,rawfiles_headerline
                          									 ,mdm_fhcreate
                          									 ,mdm_processname)
        	VALUES(  ${ReplaceSQLStringNulls(p_RAWFilesUse_id,50)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_Id,50)}
          			 ,${ReplaceSQLStringNulls(p_Process_Id,200)}
          			 ,${ReplaceSQLStringNulls(p_ProcessExec_Id,50)}
                 ,$p_RAWFilesUse_Year
                 ,$p_RAWFilesUse_Month
                 ,$p_RAWFilesUse_Day
                 ,$p_RAWFilesUse_Hour
                 ,$p_RAWFilesUse_Minute
                 ,$p_RAWFilesUse_Second
                 ,${ReplaceSQLStringNulls(p_RAWFilesUse_params,1000)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_fullName,1000)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_fullPath,1000)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_numRows,50)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_HeaderLine,4000)}
          			 ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
          			 ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )          			 
        """)
    
    
    ExecResult
  }
  
  
  private def control_ProcessExecParams_add (p_processExec_id: String
                             ,p_processExecParams_Name: String
                             ,p_processExecParams_Value: String
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_processexecparams (processexec_id
                              								  ,processexecparams_name
                              								  ,processexecparams_value
                              								  ,mdm_fhcreate
                              								  ,mdm_processname)
        	VALUES( ${ReplaceSQLStringNulls(p_processExec_id,50)}
          		  ,${ReplaceSQLStringNulls(p_processExecParams_Name,500)}
          		  ,${ReplaceSQLStringNulls(p_processExecParams_Value,4000)}
          		  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
          		  ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )          		  
        """)
    
    
    ExecResult
  }
  
  
  /**
   * Get Backup to delete
   */
  def control_getBackupToDelete(numBackupToMaintain: Int): Boolean = {
    val Result: Boolean = true
    
    if (huemulBigDataGov.RegisterInControl) {
      this.NewStep(s"Backup: get Backups info")
      val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
            select  tablesuse_id
              		, table_id
              		, tableuse_pathbackup
              		, mdm_fhcreate
            from control_tablesuse
            where tableuse_backupstatus = 1
            order by table_id, mdm_fhcreate desc
        """)
      
      this.NewStep(s"Backup: Num regs: ${ExecResultTable.ResultSet.length } ")
      var table_id_ant: String = ""
      var numCount: Int = 0
      ExecResultTable.ResultSet.foreach { x =>  
        val backup_table_id = x.getAs[String]("table_id")
        if (table_id_ant == backup_table_id) {
          numCount +=1
          
          //delete
          if (numCount > numBackupToMaintain) {
            val backupPath = x.getAs[String]("tableuse_pathbackup")
            this.NewStep(s"Backup: drop file $backupPath ")
            val backupPath_hdfs = new org.apache.hadoop.fs.Path(backupPath)
            val fs = backupPath_hdfs.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration) 
            if (fs.exists(backupPath_hdfs)){
              fs.delete(backupPath_hdfs, true)
            }   
            
           //update status
            huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s""" 
                      UPDATE control_tablesuse
                      SET tableuse_backupstatus = 2
                      WHERE tablesuse_id = ${ReplaceSQLStringNulls(x.getAs[String]("tablesuse_id"),50)}
                  """)
          }
        } else {
          table_id_ant = backup_table_id
          numCount = 1
        } 
      }
      this.NewStep(s"Backup: End Process")
    } 
    
   
    Result
  }
  
  private def control_TablesUse_add (p_Table_Name: String
                             ,p_Table_BBDDName: String
                             ,p_Process_Id: String
                             ,p_ProcessExec_Id: String
                             ,p_ProcessExecStep_Id: String
                             ,p_TableUse_Year: Integer
                             ,p_TableUse_Month: Integer
                             ,p_TableUse_Day: Integer
                             ,p_TableUse_Hour: Integer
                             ,p_TableUse_Minute: Integer
                             ,p_TableUse_Second: Integer
                             ,p_TableUse_params: String
                             ,p_TableUse_Read: Boolean
                             ,p_TableUse_Write: Boolean
                             ,p_TableUse_numRowsNew: java.lang.Long
                             ,p_TableUse_numRowsUpdate: java.lang.Long
                             ,p_TableUse_numRowsUpdatable: java.lang.Long
                             ,p_TableUse_numRowsNoChange: java.lang.Long
                             ,p_TableUse_numRowsMarkDelete: java.lang.Long
                             ,p_TableUse_numRowsExcluded: java.lang.Long
                             ,p_TableUse_numRowsTotal: java.lang.Long
                             ,p_TableUse_PartitionValue: String
                             ,p_Tableuse_pathbackup: String
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    
     //Get ExecResultRawFiles Id
    val ExecResult_TableId = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT table_id
					FROM control_tables 
					WHERE table_name = ${ReplaceSQLStringNulls(p_Table_Name,200)}
          and   table_bbddname = ${ReplaceSQLStringNulls(p_Table_BBDDName,200)}
      """)
    
    var LocalTable_id: String = null
    if (!ExecResult_TableId.IsError && ExecResult_TableId.ResultSet.length == 1){
      LocalTable_id = ExecResult_TableId.ResultSet(0).getAs[String]("table_id".toLowerCase())
    }
    
    val local_tablesuse_id = huemulBigDataGov.huemul_GetUniqueId()
      //println(s"getVersionFull: ${getVersionFull}")
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_tablesuse ( tablesuse_id
                                         ,table_id
                      								   ,process_id
                      								   ,processexec_id
                                         ,processexecstep_id
                                         ,tableuse_year
                                         ,tableuse_month
                                         ,tableuse_day
                                         ,tableuse_hour
                                         ,tableuse_minute
                                         ,tableuse_second
                                         ,tableuse_params
                      								   ,tableuse_read
                      								   ,tableuse_write
                      								   ,tableuse_numrowsnew
                      								   ,tableuse_numrowsupdate
                      								   ,tableuse_numrowsupdatable
                      								   ,tableuse_numrowsnochange 
                      								   ,tableuse_numrowsmarkdelete
                                         ${if (getVersionFull() >= 20100) " ,tableuse_numrowsexcluded" else "" /* from 2.1: */}
                      								   ,tableuse_numrowstotal
                      								   ,tableuse_partitionvalue
                                         ,tableuse_pathbackup
                                         ${if (getVersionFull() >= 20200) " ,tableuse_backupstatus" else "" /* from 2.2: */}
                      								   ,mdm_fhcreate
                      								   ,mdm_processname)
        	VALUES(  ${ReplaceSQLStringNulls(local_tablesuse_id,50)}
                 ,${ReplaceSQLStringNulls(LocalTable_id,50)}
          		   ,${ReplaceSQLStringNulls(p_Process_Id,200)}
          		   ,${ReplaceSQLStringNulls(p_ProcessExec_Id,50)}
                 ,${ReplaceSQLStringNulls(p_ProcessExecStep_Id,50)}
                 ,$p_TableUse_Year
                 ,$p_TableUse_Month
                 ,$p_TableUse_Day
                 ,$p_TableUse_Hour
                 ,$p_TableUse_Minute
                 ,$p_TableUse_Second
                 ,${ReplaceSQLStringNulls(p_TableUse_params,1000)}
          		   ,${if (p_TableUse_Read) "1" else "0"}
          		   ,${if (p_TableUse_Write) "1" else "0"}
          		   ,$p_TableUse_numRowsNew
          		   ,$p_TableUse_numRowsUpdate
          		   ,$p_TableUse_numRowsUpdatable
          		   ,$p_TableUse_numRowsNoChange
          		   ,$p_TableUse_numRowsMarkDelete
          		   ${if (getVersionFull() >= 20100) s",$p_TableUse_numRowsExcluded" else "" /* from 2.1: */}
          		   ,$p_TableUse_numRowsTotal
          		   ,${ReplaceSQLStringNulls(p_TableUse_PartitionValue,200)}
          		   ,${ReplaceSQLStringNulls(p_Tableuse_pathbackup,1000)}
          		   ${if (getVersionFull() >= 20200) s",${if (p_Tableuse_pathbackup == null || p_Tableuse_pathbackup.trim() == "") "0" else "1"}" else "" /* from 2.2: */}
          		   ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
          		   ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )          		   
        """)
    ExecResult
  }
  
  /**
   * New from 2.1
   * Used to get current version of control model
   */
  private def control_SearchtVersion() {
    try {
      val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT version_mayor
                ,version_minor
                ,version_patch
					FROM control_config
          WHERE config_id = 1
      """)
      
      if (ExecResult.IsError) {
        huemulBigDataGov.logMessageError("control version table not found...")
      } else if (ExecResult.ResultSet.length == 0){
        //insert 2.1
        huemulBigDataGov.logMessageInfo("control version: insert new version")
        val ExecResultCol = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          INSERT INTO control_config (config_id, version_mayor, version_minor, version_patch)
          VALUES (1,2,6,1)
          """)
          
          _version_mayor = 2
          _version_minor = 1
          _version_patch = 0
      } else {
        _version_mayor = ExecResult.GetValue("version_mayor", ExecResult.ResultSet(0)).toString.toInt
        _version_minor = ExecResult.GetValue("version_minor", ExecResult.ResultSet(0)).toString.toInt
        _version_patch = ExecResult.GetValue("version_patch", ExecResult.ResultSet(0)).toString.toInt
      }
      
      
    
    } catch {
      case e: Exception => 
        huemulBigDataGov.logMessageError(s"control version error_ $e")
    }
    
    if (getVersionFull() > 0)
      huemulBigDataGov.logMessageInfo(s"control model version compatibility: ${getVersionFull()} ")
      
  }
  
  def getVersionMayor(): Int =  _version_mayor
  def getVersionMinor(): Int =  _version_minor
  def getVersionPatch(): Int =  _version_patch
  def getVersionFull(): Int =  s"""${"%02d".format(_version_mayor)}${"%02d".format(_version_minor)}${"%02d".format(_version_patch)}""".toInt
  
  /**
   * New from 2.0
   * Used to update fields at the end of process
   */
  private def control_Tables_UpdateAtEnd (p_Table_BBDDName: String
                             ,p_Table_Name: String
                             ,p_table_dq_isused: Int
                             ,p_table_ovt_isused: Int
                             ,p_table_autoIncUpdate: Long
                             ,p_table_sqlScript: String
      ): huemul_JDBCResult =  {
    
    val ExecResult_TableId = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT table_id
					FROM control_tables 
					WHERE table_name = ${ReplaceSQLStringNulls(p_Table_Name,200)}
          and   table_bbddname = ${ReplaceSQLStringNulls(p_Table_BBDDName,200)}
      """)
    
    var ExecResult: huemul_JDBCResult = null
    
    var LocalTable_id: String = null
    if (!ExecResult_TableId.IsError && ExecResult_TableId.ResultSet.length == 1){
      LocalTable_id = ExecResult_TableId.ResultSet(0).getAs[String]("table_id".toLowerCase())
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_tables 
          SET table_autoincupdate   = $p_table_autoIncUpdate
             ,table_dq_isused       = case when table_dq_isused = 1 then 1 else $p_table_dq_isused end
             ,table_ovt_isused      = case when table_ovt_isused = 1 then 1 else $p_table_ovt_isused end
             ,table_sqlcreate       = ${ReplaceSQLStringNulls(p_table_sqlScript,4000)}
          WHERE table_id = ${ReplaceSQLStringNulls(LocalTable_id,50)}
          """)
    }
                    
    
    ExecResult
  }
  
  private def control_Tables_addOrUpd (p_Table_id: String
                             ,p_Table_BBDDName: String
                             ,p_Table_Name: String
                             ,p_Table_Description: String
                             ,p_Table_BusinessOwner: String
                             ,p_Table_ITOwner: String
                             ,p_Table_PartitionField: String
                             ,p_Table_TableType: String
                             ,p_Table_StorageType: String
                             ,p_Table_LocalPath: String
                             ,p_Table_GlobalPath: String
                             ,p_table_fullname_dq: String
                             ,p_table_fullname_ovt: String
                             ,p_Table_SQLCreate: String
                             ,p_Table_Frequency: String
                             ,p_table_backup: Boolean
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    var table_autoIncUpdate: Integer = 0
     //Get ExecResultRawFiles Id
    val ExecResult_TableId = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT table_id, table_autoincupdate
					FROM control_tables 
					WHERE table_name = ${ReplaceSQLStringNulls(p_Table_Name,200)}
          and   table_bbddname = ${ReplaceSQLStringNulls(p_Table_BBDDName,200)}
      """)
    
    var ExecResult: huemul_JDBCResult = null
    
    var LocalTable_id: String = null
    if (!ExecResult_TableId.IsError && ExecResult_TableId.ResultSet.length == 1){
      LocalTable_id = ExecResult_TableId.ResultSet(0).getAs[String]("table_id".toLowerCase())
      table_autoIncUpdate = ExecResult_TableId.GetValue("table_autoIncUpdate".toLowerCase(),ExecResult_TableId.ResultSet(0)).toString.toInt + 1  //.ResultSet(0).getAs[Int]("table_autoIncUpdate".toLowerCase()) + 1
      
      val ExecResultCol = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_columns 
          SET mdm_active = 0 
          WHERE table_id = ${ReplaceSQLStringNulls(LocalTable_id,50)}
          """)
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_tables 
          SET table_description     = CASE WHEN mdm_manualchange = 1 THEN table_description		ELSE ${ReplaceSQLStringNulls(p_Table_Description,1000)}	END	 
        		 ,table_businessowner	  = CASE WHEN mdm_manualchange = 1 THEN table_businessowner	ELSE ${ReplaceSQLStringNulls(p_Table_BusinessOwner,200)}	END
        		 ,table_itowner			    = CASE WHEN mdm_manualchange = 1 THEN table_itowner			  ELSE ${ReplaceSQLStringNulls(p_Table_ITOwner,200)}		END
        		 ,table_partitionfield	= ${ReplaceSQLStringNulls(p_Table_PartitionField,200)}	
        		 ,table_tabletype		    = ${ReplaceSQLStringNulls(p_Table_TableType,50)}		
        		 ,table_storagetype		  = ${ReplaceSQLStringNulls(p_Table_StorageType,50)}		
        		 ,table_localpath		    = ${ReplaceSQLStringNulls(p_Table_LocalPath,1000)}		
        		 ,table_globalpath		  = ${ReplaceSQLStringNulls(p_Table_GlobalPath,1000)}
             ,table_fullname_dq     = ${ReplaceSQLStringNulls(p_table_fullname_dq,1200)}            
             ,table_fullname_ovt    = ${ReplaceSQLStringNulls(p_table_fullname_ovt,1200)}
        		 ,table_sqlcreate    		= ${ReplaceSQLStringNulls(p_Table_SQLCreate,4000)}		
        		 ,table_frequency		    = ${ReplaceSQLStringNulls(p_Table_Frequency,200)}
        		 ,table_backup          = ${if (p_table_backup) "1" else "0"}
          WHERE table_id = ${ReplaceSQLStringNulls(LocalTable_id,50)}
          """)
          
/*excluded, the fields are updated at the end
             ,table_dq_isused       = ${p_table_dq_isused}             
             ,table_ovt_isused      = ${p_table_ovt_isused}
             ,table_autoincupdate   = ${ReplaceSQLStringNulls(table_autoIncUpdate.toString)}
             *              
             */

          
      ExecResult.OpenVar = LocalTable_id
      ExecResult.OpenVar2 = table_autoIncUpdate.toString
    } else {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_tables ( table_id
                      								,area_id
                      								,table_bbddname
                      								,table_name
                      								,table_description
                      								,table_businessowner
                      								,table_itowner
                      								,table_partitionfield
                      								,table_tabletype
                      								,table_storagetype
                      								,table_localpath
                      								,table_globalpath
                                      ,table_fullname_dq
                                      ,table_dq_isused
                                      ,table_fullname_ovt
                                      ,table_ovt_isused
                      								,table_sqlcreate
                      								,table_frequency
                                      ,table_autoincupdate
                                      ,table_backup
                      								,mdm_fhcreate
                      								,mdm_processname) 	
        	VALUES(   ${ReplaceSQLStringNulls(p_Table_id,50)}
            			,'0'
            			,${ReplaceSQLStringNulls(p_Table_BBDDName,200)}
            			,${ReplaceSQLStringNulls(p_Table_Name,200)}
            			,${ReplaceSQLStringNulls(p_Table_Description,1000)}
            			,${ReplaceSQLStringNulls(p_Table_BusinessOwner,200)}
            			,${ReplaceSQLStringNulls(p_Table_ITOwner,200)}
            			,${ReplaceSQLStringNulls(p_Table_PartitionField,200)}
            			,${ReplaceSQLStringNulls(p_Table_TableType,50)}
            			,${ReplaceSQLStringNulls(p_Table_StorageType,50)}
            			,${ReplaceSQLStringNulls(p_Table_LocalPath,1000)}
            			,${ReplaceSQLStringNulls(p_Table_GlobalPath,1000)}            			
                  ,${ReplaceSQLStringNulls(p_table_fullname_dq,1200)}
                  ,0
                  ,${ReplaceSQLStringNulls(p_table_fullname_ovt,1200)}
                  ,0
            			,${ReplaceSQLStringNulls(p_Table_SQLCreate,4000)}
            			,${ReplaceSQLStringNulls(p_Table_Frequency,200)}
            			,1
                  ,${if (p_table_backup) "1" else "0"}
            			,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
            			,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )            			
          """)
      ExecResult.OpenVar = p_Table_id
      ExecResult.OpenVar2 = "1"
    }
    
    
    ExecResult
  }
  
  
  private def control_Columns_addOrUpd (p_Table_id: String
                             ,p_Column_id: String
                             ,p_Column_Position: Integer
                             ,p_Column_Name: String
                             ,p_Column_Description: String
                             ,p_Column_Formula: String
                             ,p_Column_DataType: String
                             ,p_Column_DataTypeDeploy: String
                             ,p_Column_SensibleData: Boolean
                             ,p_Column_EnableDTLog: Boolean
                             ,p_Column_EnableOldValue: Boolean
                             ,p_Column_EnableProcessLog: Boolean
                             ,p_Column_EnableOldValueTrace: Boolean
                             ,p_Column_DefaultValue: String
                             ,p_Column_SecurityLevel: String
                             ,p_Column_Encrypted: String
                             ,p_Column_ARCO: Boolean
                             ,p_Column_Nullable: Boolean
                             ,p_Column_IsPK: Boolean
                             ,p_Column_IsUnique: Boolean
                             ,p_Column_DQ_MinLen: Integer
                             ,p_Column_DQ_MaxLen: Integer
                             ,p_Column_DQ_MinDecimalValue: Decimal
                             ,p_Column_DQ_MaxDecimalValue: Decimal
                             ,p_Column_DQ_MinDateTimeValue: String
                             ,p_Column_DQ_MaxDateTimeValue: String
                             ,p_Column_DQ_RegExp: String
                             ,p_Column_BusinessGlossary: String
                             ,p_MDM_ProcessName: String

                             
      ): huemul_JDBCResult =  {
    
     //Get Column Id
    val ExecResult_ColumnId = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT column_id
					FROM control_columns 
					WHERE table_id = ${ReplaceSQLStringNulls(p_Table_id,50)}
          and   column_name = ${ReplaceSQLStringNulls(p_Column_Name,200)}
      """)
    
    var ExecResult: huemul_JDBCResult = null
    var Localcolumn_id: String = null
    if (!ExecResult_ColumnId.IsError && ExecResult_ColumnId.ResultSet.length == 1){
      Localcolumn_id = ExecResult_ColumnId.ResultSet(0).getAs[String]("column_id".toLowerCase())
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_columns 
          SET column_position	            = $p_Column_Position
        		 ,column_description			    = CASE WHEN mdm_manualchange = 1 THEN column_description ELSE ${ReplaceSQLStringNulls(p_Column_Description,1000)}	END
        		 ,column_formula				      = CASE WHEN mdm_manualchange = 1 THEN column_formula ELSE ${ReplaceSQLStringNulls(p_Column_Formula,1000)}	END			
        		 ,column_datatype				      = ${ReplaceSQLStringNulls(p_Column_DataType,50)}
        	${if (getVersionFull() >= 20500) s",column_datatypedeploy = ${ReplaceSQLStringNulls(p_Column_DataTypeDeploy,50)} " else ""}	 
        		 ,column_sensibledata			    = CASE WHEN mdm_manualchange = 1 THEN column_sensibledata ELSE ${if (p_Column_SensibleData) "1" else "0"}	END		
        		 ,column_enabledtlog			    = ${if (p_Column_EnableDTLog) "1" else "0"}
        		 ,column_enableoldvalue			  = ${if (p_Column_EnableOldValue) "1" else "0"}
        		 ,column_enableprocesslog		  = ${if (p_Column_EnableProcessLog) "1" else "0"}
        		 ,column_enableoldvaluetrace  = ${if (p_Column_EnableOldValueTrace) "1" else "0"}
        		 ,column_defaultvalue			    = ${ReplaceSQLStringNulls(p_Column_DefaultValue,1000)}			
        		 ,column_securitylevel			  = CASE WHEN mdm_manualchange = 1 THEN column_securitylevel ELSE ${ReplaceSQLStringNulls(p_Column_SecurityLevel,200)}	END		
        		 ,column_encrypted				    = ${ReplaceSQLStringNulls(p_Column_Encrypted,200)}				
        		 ,column_arco					        = CASE WHEN mdm_manualchange = 1 THEN column_arco ELSE ${if (p_Column_ARCO) "'1'" else "'0'"}	END				
        		 ,column_nullable				      = ${if (p_Column_Nullable) "1" else "0"}				
        		 ,column_ispk					        = ${if (p_Column_IsPK) "1" else "0"}
        		 ,column_isunique				      = ${if (p_Column_IsUnique) "1" else "0"}
        		 ,column_dq_minlen				    = $p_Column_DQ_MinLen
        		 ,column_dq_maxlen				    = $p_Column_DQ_MaxLen
        		 ,column_dq_mindecimalvalue		= $p_Column_DQ_MinDecimalValue
        		 ,column_dq_maxdecimalvalue		= $p_Column_DQ_MaxDecimalValue
        		 ,column_dq_mindatetimevalue	= ${ReplaceSQLStringNulls(p_Column_DQ_MinDateTimeValue,null)}	
        		 ,column_dq_maxdatetimevalue	= ${ReplaceSQLStringNulls(p_Column_DQ_MaxDateTimeValue,null)}	
        		 ,column_dq_regexp            = ${ReplaceSQLStringNulls(p_Column_DQ_RegExp,1000)}
        		 ,column_businessglossary      = CASE WHEN mdm_manualchange = 1 THEN column_businessglossary ELSE ${ReplaceSQLStringNulls(p_Column_BusinessGlossary,100)}	END
        		 ,mdm_active					        = 1
          WHERE column_id = ${ReplaceSQLStringNulls(Localcolumn_id,null)}
          """)
          
      ExecResult.OpenVar = Localcolumn_id
    } else {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_columns ( column_id
                      								 ,table_id
                      								 ,column_position
                      								 ,column_name
                      								 ,column_description
                      								 ,column_formula
                      								 ,column_datatype
    ${if (getVersionFull() >= 20500) s",column_datatypedeploy" else ""}	 
                      								 ,column_sensibledata
                      								 ,column_enabledtlog
                      								 ,column_enableoldvalue
                      								 ,column_enableprocesslog
                                       ,column_enableoldvaluetrace
                      								 ,column_defaultvalue
                      								 ,column_securitylevel
                      								 ,column_encrypted
                      								 ,column_arco
                      								 ,column_nullable
                      								 ,column_ispk
                      								 ,column_isunique
                      								 ,column_dq_minlen
                      								 ,column_dq_maxlen
                      								 ,column_dq_mindecimalvalue
                      								 ,column_dq_maxdecimalvalue
                      								 ,column_dq_mindatetimevalue
                      								 ,column_dq_maxdatetimevalue
                      								 ,column_dq_regexp
                                       ,column_businessglossary
                      								 ,mdm_fhcreate
                      								 ,mdm_processname
                                       ,mdm_manualchange
                                       ,mdm_active) 	
        	VALUES(    ${ReplaceSQLStringNulls(p_Column_id,50)}
            			 ,${ReplaceSQLStringNulls(p_Table_id,50)}
            			 ,$p_Column_Position
            			 ,${ReplaceSQLStringNulls(p_Column_Name,200)}
            			 ,${ReplaceSQLStringNulls(p_Column_Description,1000)}
            			 ,${ReplaceSQLStringNulls(p_Column_Formula,1000)}
            			 ,${ReplaceSQLStringNulls(p_Column_DataType,50)}
 ${if (getVersionFull() >= 20500) s",${ReplaceSQLStringNulls(p_Column_DataTypeDeploy,50)}" else ""}	             			 
            			 ,${if (p_Column_SensibleData) "1" else "0"}
            			 ,${if (p_Column_EnableDTLog) "1" else "0"}
            			 ,${if (p_Column_EnableOldValue) "1" else "0"}
            			 ,${if (p_Column_EnableProcessLog) "1" else "0"}
            			 ,${if (p_Column_EnableOldValueTrace) "1" else "0"}
            			 ,${ReplaceSQLStringNulls(p_Column_DefaultValue,1000)}
            			 ,${ReplaceSQLStringNulls(p_Column_SecurityLevel,200)}
            			 ,${ReplaceSQLStringNulls(p_Column_Encrypted,200)}
            			 ,${if (p_Column_ARCO) "'1'" else "'0'"}
            			 ,${if (p_Column_Nullable) "1" else "0"}
            			 ,${if (p_Column_IsPK) "1" else "0"}
            			 ,${if (p_Column_IsUnique) "1" else "0"}
            			 ,$p_Column_DQ_MinLen
            			 ,$p_Column_DQ_MaxLen
            			 ,$p_Column_DQ_MinDecimalValue
            			 ,$p_Column_DQ_MaxDecimalValue
            			 ,${ReplaceSQLStringNulls(p_Column_DQ_MinDateTimeValue,null)}
            			 ,${ReplaceSQLStringNulls(p_Column_DQ_MaxDateTimeValue,null)}
            			 ,${ReplaceSQLStringNulls(p_Column_DQ_RegExp,1000)}
            			 ,${ReplaceSQLStringNulls(p_Column_BusinessGlossary,100)}
            			 ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
            			 ,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
            			 ,0
                   ,1
      )
          """)
          
      ExecResult.OpenVar = p_Column_id
    }
    
    
    ExecResult
  }
  
  
  private def control_TablesRel_add (p_TableRel_id: String
                             ,p_FK_ID: String
                             ,p_Table_NamePK: String
                             ,p_Table_BBDDPK: String
                             ,p_TableFK_NameRelationship: String
                             ,p_TableRel_ValidNull: Boolean
                             ,p_mdm_processname: String
      ): huemul_JDBCResult =  {
    
     //Get Column Id
    val ExecResult_PK_Id = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT table_id 
          FROM control_tables 
  			  WHERE table_name = ${ReplaceSQLStringNulls(p_Table_NamePK,200)}
  			  AND   table_bbddname = ${ReplaceSQLStringNulls(p_Table_BBDDPK,200)}
      """)
    
    var ExecResult: huemul_JDBCResult = null
    var PK_Id: String = null
    if (!ExecResult_PK_Id.IsError && ExecResult_PK_Id.ResultSet.length == 1){
      PK_Id = ExecResult_PK_Id.ResultSet(0).getAs[String]("table_id".toLowerCase())
      
      val ExecResult_Rel_Id = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT tablerel_id 
					 FROM control_tablesrel 
					 WHERE table_idpk = ${ReplaceSQLStringNulls(PK_Id,50)}
					 AND   table_idfk = ${ReplaceSQLStringNulls(p_FK_ID,50)}
					 AND   tablefk_namerelationship = ${ReplaceSQLStringNulls(p_TableFK_NameRelationship,100)}
      """)
      
      if (ExecResult_Rel_Id.IsError)
        RaiseError(ExecResult_Rel_Id.ErrorDescription)
        
      var TableRel_id: String = null
      if (ExecResult_Rel_Id.ResultSet.length > 0){
        TableRel_id = ExecResult_Rel_Id.ResultSet(0).getAs[String]("tablerel_id".toLowerCase())
      
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          DELETE FROM control_tablesrelcol WHERE tablerel_id = ${ReplaceSQLStringNulls(TableRel_id,50)}
          """)
          
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_tablesrel 
          SET  tablerel_validNull       = ${if (p_TableRel_ValidNull) "1" else "0"}
          WHERE tablerel_id = ${ReplaceSQLStringNulls(TableRel_id,50)}
          """)
          
        ExecResult.OpenVar = TableRel_id
        ExecResult.OpenVar2 = PK_Id
      } else {
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_tablesrel (tablerel_id
                      							   , table_idpk
                      							   , table_idfk
                      							   , tablefk_namerelationship
                                       , tablerel_validnull
                                       , mdm_manualchange
                      							   , mdm_fhcreate
                      							   , mdm_processname) 	
      		VALUES( ${ReplaceSQLStringNulls(p_TableRel_id,50)}
      			   , ${ReplaceSQLStringNulls(PK_Id,50)}
      			   , ${ReplaceSQLStringNulls(p_FK_ID,50)}
      			   , ${ReplaceSQLStringNulls(p_TableFK_NameRelationship,100)}
      			   , ${if (p_TableRel_ValidNull) "1" else "0"}
      			   , 0
      			   , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
      			   , ${ReplaceSQLStringNulls(p_mdm_processname,200)}
          )      			   
          """)
        ExecResult.OpenVar = p_TableRel_id
        ExecResult.OpenVar2 = PK_Id
      }
    }
    
    
    
    ExecResult
  }
  
  
  
  private def control_TablesRelCol_add (p_TableRel_id: String
                             ,p_PK_ID: String
                             ,p_ColumnName_PK: String
                             ,p_FK_ID: String
                             ,p_ColumnName_FK: String
                             ,p_mdm_processname: String
      ): huemul_JDBCResult =  {
    
     //Get Column Id PK
    val ExecResult_ColumnPK_Id = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT column_id 
          FROM control_columns
				  WHERE table_id    = ${ReplaceSQLStringNulls(p_PK_ID,50)}
				  AND   column_name = ${ReplaceSQLStringNulls(p_ColumnName_PK,200)}
      """)
    
    var ExecResult: huemul_JDBCResult = null
    var ColumnPK: String = null
    if (!ExecResult_ColumnPK_Id.IsError && ExecResult_ColumnPK_Id.ResultSet.length == 1){
      ColumnPK = ExecResult_ColumnPK_Id.ResultSet(0).getAs[String]("column_id".toLowerCase())
    }
    
    //Get Column Id FK
    val ExecResult_ColumnFK_Id = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT column_id 
          FROM control_columns
				  WHERE table_id    = ${ReplaceSQLStringNulls(p_FK_ID,50)}
				  AND   column_name = ${ReplaceSQLStringNulls(p_ColumnName_FK,200)}
      """)
    
    var ColumnFK: String = null
    if (!ExecResult_ColumnFK_Id.IsError && ExecResult_ColumnFK_Id.ResultSet.length == 1){
      ColumnFK = ExecResult_ColumnFK_Id.ResultSet(0).getAs[String]("column_id".toLowerCase())
    }
    
    
    ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_tablesrelcol (tablerel_id
                        									 ,column_idfk
                        									 ,column_idpk
                        									 ,mdm_fhcreate
                        									 ,mdm_processname
        									 )
        	VALUES( ${ReplaceSQLStringNulls(p_TableRel_id,50)}
          		  ,${ReplaceSQLStringNulls(ColumnFK,50)}
          		  ,${ReplaceSQLStringNulls(ColumnPK,50)}
          		  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
          		  ,${ReplaceSQLStringNulls(p_mdm_processname,200)}
          )          		  
      """)
      
    
    
    
    ExecResult
  }
  
  
  private def control_Query_add (p_query_id: String
                             ,p_processexecstep_id: String
                             ,p_processexec_id: String
                             ,p_rawfiles_id: String
                             ,p_rawFilesDet_id: String
                             ,p_table_id: String
                             ,p_query_alias: String
                             ,p_query_sql_from: String
                             ,p_query_sql_where: String
                             ,p_query_numErrors: Integer
                             ,p_query_autoinc: Integer
                             ,p_query_israw: Boolean
                             ,p_query_isfinaltable: Boolean
                             ,p_query_isquery: Boolean
                             ,p_query_isreference: Boolean
                             ,p_query_numrows_real: Long
                             ,p_query_numrows_expected: Long
                             ,p_query_duration_hour: Long
                             ,p_query_duration_min: Long
                             ,p_query_duration_sec: Long
                             ,p_error_id: String
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    val newQuerys = new huemul_control_query()
    newQuerys.query_id = p_query_id
    newQuerys.tableAlias_name = p_query_alias
    newQuerys.table_name = p_query_alias
    newQuerys.rawFilesDet_Id = p_rawFilesDet_id
    newQuerys.isRAW = p_query_israw
    newQuerys.isTable = p_query_isfinaltable
    newQuerys.isTemp = !p_query_israw && !p_query_isfinaltable
    control_QueryArray.append(newQuerys)
    
    var ExecResult: huemul_JDBCResult = null
    
    ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_query  ( query_id
      								                ,processexecstep_id
                                      ,processexec_id      
                                      ,rawfiles_id 
                                      ,rawFilesDet_id
                                      ,table_id 
                                      ,query_alias
                                      ,query_sql_from	
                                      ,query_sql_where
                                      ,query_numErrors
                                      ,query_autoinc
                                      ,query_israw	
                                      ,query_isfinaltable	
                                      ,query_isquery
                                      ,query_isreferenced
                                      ,query_numrows_real
                                      ,query_numrows_expected 
                                      ,query_duration_hour
                                      ,query_duration_min	
                                      ,query_duration_sec						 
                                      ,error_id 
                                      ,mdm_fhcreate
                                      ,mdm_processname  ) 	
        	VALUES(   ${ReplaceSQLStringNulls(p_query_id,50)}
            			,${ReplaceSQLStringNulls(p_processexecstep_id,50)}
            			,${ReplaceSQLStringNulls(p_processexec_id,50)}
            			,${ReplaceSQLStringNulls(p_rawfiles_id,50)}
            			,${ReplaceSQLStringNulls(p_rawFilesDet_id,50)}
            			,${ReplaceSQLStringNulls(p_table_id,50)}
            			,${ReplaceSQLStringNulls(p_query_alias,200)}
            			,${ReplaceSQLStringNulls(p_query_sql_from,4000)}
            			,${ReplaceSQLStringNulls(p_query_sql_where,4000)}
            			,$p_query_numErrors
            			,$p_query_autoinc
            			,${if (p_query_israw) "1" else "0"}
            			,${if (p_query_isfinaltable) "1" else "0"}
            			,${if (p_query_isquery) "1" else "0"}
                  ,${if (p_query_isreference) "1" else "0"}
            			,$p_query_numrows_real
            			,$p_query_numrows_expected
                  ,$p_query_duration_hour
                  ,$p_query_duration_min
                  ,$p_query_duration_sec
                  ,${ReplaceSQLStringNulls(p_error_id,null)}
            			,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
            			,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )            			
          """)
              
    ExecResult
  }
  
  
  private def control_QueryColumn_add (p_querycol_id: String
                             ,p_query_id: String
                             ,p_rawfilesdet_id: String
                             ,p_column_id: String
                             ,p_querycol_pos: Integer
                             ,p_querycol_name: String
                             ,p_querycol_sql: String
                             ,p_querycol_posstart: Integer
                             ,p_querycol_posend: Integer
                             ,p_querycol_line: Integer
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    
    
    var ExecResult: huemul_JDBCResult = null
    
    val newReg = new huemul_control_querycol()
    newReg.column_id = p_column_id
    newReg.query_id = p_query_id
    newReg.querycol_id = p_querycol_id
    newReg.querycol_name = p_querycol_name
    newReg.rawfilesdet_id = p_rawfilesdet_id
    control_QueryColArray.append(newReg)
    
    ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_querycolumn  ( querycol_id
                        									  ,query_id
                        									  ,rawfilesdet_id
                        									  ,column_id
                                            ,querycol_pos
                        									  ,querycol_name
                        									  ,querycol_sql
                        									  ,querycol_posstart
                        									  ,querycol_posend
                        									  ,querycol_line
                        									  ,mdm_fhcreate 
                                            ,mdm_processname ) 	
        	VALUES(  ${ReplaceSQLStringNulls(p_querycol_id,50)}
            			,${ReplaceSQLStringNulls(p_query_id,50)}
            			,${ReplaceSQLStringNulls(p_rawfilesdet_id,50)}
            			,${ReplaceSQLStringNulls(p_column_id,50)}
            			,$p_querycol_pos
            			,${ReplaceSQLStringNulls(p_querycol_name,200)}
            			,${ReplaceSQLStringNulls(p_querycol_sql,4000)}
            			,$p_querycol_posstart
            			,$p_querycol_posend
            			,$p_querycol_line
            			,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
            			,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )            			
          """)
              
    ExecResult
  }
  
  private def control_QueryColumnOri_add ( p_querycolori_id: String       
                      										,p_querycol_id: String
                      										,p_table_idori: String
                      										,p_column_idori: String
                      										,p_rawfilesdet_idori: String
                      										,p_rawfilesdetfields_idori: String
                      										,p_query_idori: String
                      										,p_querycol_idori: String
                      										,p_querycolori_dbname: String
                      										,p_querycolori_tabname: String
                      										,p_querycolori_tabalias: String
                      										,p_querycolori_colname: String	
                      										,p_querycolori_isselect: Boolean
                      										,p_querycolori_iswhere: Boolean
                      										,p_querycolori_ishaving: Boolean
                      										,p_querycolori_isorder: Boolean
                                          ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    
    
    var ExecResult: huemul_JDBCResult = null
    
    ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_querycolumnori  ( querycolori_id       
                        										,querycol_id
                        										,table_idori
                                            ,column_idori
                        										,rawfilesdet_idori
                                            ,rawfilesdetfields_idori
                        										,query_idori
                                            ,querycol_idori
                        										,querycolori_dbname
                        										,querycolori_tabname
                        										,querycolori_tabalias
                        										,querycolori_colname		
                        										,querycolori_isselect	
                        										,querycolori_iswhere	
                        										,querycolori_ishaving	
                        										,querycolori_isorder
                        										,mdm_fhcreate  
                                            ,mdm_processname ) 	
        	VALUES(  ${ReplaceSQLStringNulls(p_querycolori_id,50)}
            			,${ReplaceSQLStringNulls(p_querycol_id,50)}
            			,${ReplaceSQLStringNulls(p_table_idori,50)}
            			,${ReplaceSQLStringNulls(p_column_idori,50)}
            			,${ReplaceSQLStringNulls(p_rawfilesdet_idori,50)}
            			,${ReplaceSQLStringNulls(p_rawfilesdetfields_idori,50)}
            			,${ReplaceSQLStringNulls(p_query_idori,50)}
            			,${ReplaceSQLStringNulls(p_querycol_idori,50)}
            			,${ReplaceSQLStringNulls(p_querycolori_dbname,200)}
            			,${ReplaceSQLStringNulls(p_querycolori_tabname,200)}
            			,${ReplaceSQLStringNulls(p_querycolori_tabalias,200)}
            			,${ReplaceSQLStringNulls(p_querycolori_colname,200)}
            			,${if (p_querycolori_isselect) "1" else "0"}
            			,${if (p_querycolori_iswhere) "1" else "0"}
            			,${if (p_querycolori_ishaving) "1" else "0"}
            			,${if (p_querycolori_isorder) "1" else "0"}
            			,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
            			,${ReplaceSQLStringNulls(p_MDM_ProcessName,200)}
      )            			
          """)
              
    ExecResult
  }
  
  
  
 
  
  
  
  
  private def control_Error_finish (p_processExec_id: String
                                   ,p_processExecStep_id: String
                                   ,p_Error_Id: String
                                   ,p_Error_Message: String
                                   ,p_Error_Code: Integer
                                   ,p_Error_Trace: String
                                   ,p_Error_ClassName: String
                                   ,p_Error_FileName: String
                                   ,p_Error_LIneNumber: String
                                   ,p_Error_MethodName: String
                                   ,p_Error_Detail: String
                                   ,p_MDM_ProcessName  : String                                 
      ): huemul_JDBCResult =  {
    
    val ExecResult = control_Error_register(p_Error_Id
                                   ,p_Error_Message
                                   ,p_Error_Code
                                   ,p_Error_Trace
                                   ,p_Error_ClassName
                                   ,p_Error_FileName
                                   ,p_Error_LIneNumber
                                   ,p_Error_MethodName
                                   ,p_Error_Detail
                                   ,p_MDM_ProcessName)
      
    control_processExec_Finish(p_processExec_id, p_processExecStep_id, p_Error_Id)
      
    
    ExecResult
  }
  
  private def control_Error_register (p_Error_Id: String
                                   ,p_Error_Message: String
                                   ,p_Error_Code: Integer
                                   ,p_Error_Trace: String
                                   ,p_Error_ClassName: String
                                   ,p_Error_FileName: String
                                   ,p_Error_LIneNumber: String
                                   ,p_Error_MethodName: String
                                   ,p_Error_Detail: String
                                   ,p_MDM_ProcessName  : String                                 
      ): huemul_JDBCResult =  {
    
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
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
        	VALUES( ${ReplaceSQLStringNulls(p_Error_Id,50)}
          		  ,${ReplaceSQLStringNulls(p_Error_Message,4000)}
                ,$p_Error_Code
          		  ,${ReplaceSQLStringNulls(p_Error_Trace,4000)}
          		  ,${ReplaceSQLStringNulls(p_Error_ClassName,100)}
          		  ,${ReplaceSQLStringNulls(p_Error_FileName,500)}
          		  ,${ReplaceSQLStringNulls(p_Error_LIneNumber,100)}
          		  ,${ReplaceSQLStringNulls(p_Error_MethodName,100)}
          		  ,${ReplaceSQLStringNulls(p_Error_Detail,500)}
          		  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime(),null)}
          		  ,${ReplaceSQLStringNulls(p_MDM_ProcessName,1000)}
          )          		  
      """)
       
    
    ExecResult
  }

  /** Get and save Spark Environment Parameters in controls database
   *
   *  @author    Christian.Sattler@gmail.com
   *  @since     2.[6]
   *  @group     control
   *
   *  @return    true is save was success, false on error/TestPlanMode enabled/RegisterInControl disable
   */
  def saveSparkEnvParams(): Boolean = {
    // Only get Spark Environment parameters on Non-TestPlanMode run
    if (huemulBigDataGov.TestPlanMode) return false

    // Only on root control_Id will be register the parameters (Control_IdParent==null)
    if (Control_IdParent!=null) return false

    huemulBigDataGov.logMessageInfo("Getting Spark Environment Parameters")
    val controlDB=new huemul_ControlModel(huemulBigDataGov)

    // HashMap that stores the execution parameters for each category (runtime, sparkProperties & systemProperties)
    var envParamHash:HashMap[String, HashMap[String,String]] = new HashMap[String,HashMap[String,String]]()

    //Spark Api Rest url
    val urlSparkApi = s"${huemulBigDataGov.IdPortMonitoring}/api/v1/applications/${huemulBigDataGov.IdApplication}/environment"

    try {
      val restGet = Source.fromURL(urlSparkApi)
      val jsonData:String = restGet.mkString //retrieve JsonData
      restGet.close()

      val json = JSON.parseFull(jsonData)
      val jsonMap:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]

      // Get runtime Values
      envParamHash += ("runtime" -> new HashMap[String,String]())
      val dataRuntime= jsonMap("runtime").asInstanceOf[Map[String,Any]]
      dataRuntime.keys.toList.foreach(f => envParamHash("runtime") += (f -> dataRuntime(f).toString))

      // Get sparkProperties values
      envParamHash += ("sparkProperties" -> new HashMap[String,String]())
      val dataSparkProperties =  jsonMap("sparkProperties").asInstanceOf[List[Any]]
      dataSparkProperties.foreach(f => envParamHash("sparkProperties") += (f.asInstanceOf[List[String]].head -> f.asInstanceOf[List[String]].last ))

      // Get systemProperties values
      envParamHash += ("systemProperties" -> new HashMap[String,String]())
      val dataSystemProperties =  jsonMap("systemProperties").asInstanceOf[List[Any]]
      dataSystemProperties.foreach(f => envParamHash("systemProperties") += (f.asInstanceOf[List[String]].head -> f.asInstanceOf[List[String]].last ))

    }
    catch {
      case e: Exception =>
        huemulBigDataGov.logMessageError("Error: could not get Spark Environment Params")
        val error = new huemul_ControlError(huemulBigDataGov)
        error.GetError(e,this.getClass.getName, 9999)
        controlDB.registerError(error ,"huemulEnv")
        return false
    }

    // Save Parameter to control table
    try {

      for((categoryType, paramsHash) <- envParamHash) {
        for((envParamName,envParamValue) <- paramsHash ) {
          if (huemulBigDataGov.RegisterInControl) {
            val result: huemul_JDBCResult= controlDB.addControlProcessExecEnvDB(
              this.Control_Id
              , categoryType
              , envParamName
              , envParamValue
              , this.getClass.getName)

            //if there is an error during tabla insert, throw an error with the description
            if (result!=null && result.IsError) {
              throw new Exception(result.ErrorDescription)
            }
          }

          if (huemulBigDataGov.DebugMode){
            huemulBigDataGov.logMessageDebug(s"Environment: category: $categoryType, name: $envParamName, value: $envParamValue")
          }
        }
      }
    } catch {
      case e: Exception =>
        huemulBigDataGov.logMessageError("Error: could not save Spark Environment Params on table")
        huemulBigDataGov.logMessageError(e)
        // Note: No need to register error, controlDb has it done
        return false
    }

    if (huemulBigDataGov.RegisterInControl) huemulBigDataGov.logMessageInfo("Spark Environment Parameters saved to control")
    true
  }
  
}