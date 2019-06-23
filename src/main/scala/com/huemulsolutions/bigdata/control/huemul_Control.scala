package com.huemulsolutions.bigdata.control


import org.apache.spark.sql.types._
import java.util.Calendar;
import com.huemulsolutions.bigdata.datalake.huemul_DataLake
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel._
import huemulType_Frequency._

class huemul_Control (phuemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control, runFrequency: huemulType_Frequency, IsSingleton: Boolean = true, RegisterInControlLog: Boolean = true) extends Serializable  {
  val huemulBigDataGov = phuemulBigDataGov
  val Control_Id: String = huemulBigDataGov.huemul_GetUniqueId() 
  
  val Invoker = new Exception().getStackTrace()
  
  val Control_IdParent: String = if (ControlParent == null) null else ControlParent.Control_Id
  val Control_ClassName: String = Invoker(1).getClassName().replace("$", "")
  val Control_ProcessName: String = Invoker(1).getMethodName().replace("$", "")
  val Control_FileName: String = Invoker(1).getFileName.replace("$", "")
  
  var Control_Start_dt: Calendar = Calendar.getInstance()
  var Control_Stop_dt: Calendar = null
  val Control_Error: huemul_ControlError = new huemul_ControlError(huemulBigDataGov)
  val Control_Params: scala.collection.mutable.ListBuffer[huemul_LibraryParams] = new scala.collection.mutable.ListBuffer[huemul_LibraryParams]() 
  private var LocalIdStep: String = ""
  private var Step_IsDQ: Boolean = false
  private var AdditionalParamsInfo: String = ""
  
  private var processExec_param_others: String = ""
  private var processExec_dtStart: java.util.Calendar = huemulBigDataGov.getCurrentDateTimeJava()
  private var processExec_dtEnd: java.util.Calendar = null
  private var processExecStep_dtStart: java.util.Calendar = null
  private var _testPlanGroup_Id: String = null
  
  private val testPlanDetails: scala.collection.mutable.ListBuffer[huemul_TestPlan] = new scala.collection.mutable.ListBuffer[huemul_TestPlan]() 
  def getTestPlanDetails: scala.collection.mutable.ListBuffer[huemul_TestPlan] = {return testPlanDetails}
  
  //Find process name in control_process
  
  if (RegisterInControlLog && huemulBigDataGov.RegisterInControl) {
    control_process_addOrUpd(Control_ClassName
                  ,Control_ClassName
                  ,Control_FileName
                  ,""
                  ,runFrequency.toString()
                  ,""
                  ,Control_ClassName)     
  }
  
  
  //Insert processExcec
  phuemulBigDataGov.logMessageWarn (s"HuemulControl: processName: ${Control_ClassName}, ProcessExec_Id: ${Control_Id}")
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
        if (huemulBigDataGov.DebugMode) phuemulBigDataGov.logMessageDebug(s"HuemulControl: waiting for Singleton... (class: $Control_ClassName, appId: ${huemulBigDataGov.IdApplication} )")
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
 def getparamYear(): Integer = {return paramYear}
 
 private var paramMonth: Integer = 0
 def getparamMonth(): Integer = {return paramMonth}
 
 private var paramDay: Integer = 0
 def getparamDay(): Integer = {return paramDay}
 
 private var paramHour: Integer = 0
 def getparamHour(): Integer = {return paramHour}
 
 private var paramMin: Integer = 0
 def getparamMin(): Integer = {return paramMin}
 
 private var paramSec: Integer = 0
 def getparamSec(): Integer = {return paramSec}
 
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
    AdditionalParamsInfo = s"${AdditionalParamsInfo}${if (AdditionalParamsInfo == "") "" else ", "} {${paramName}}=${value}"
    
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
      phuemulBigDataGov.logMessageDebug(s"HuemulControl: Param num: ${Control_Params.length}, name: $name, value: $value")
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
  
    if (!huemulBigDataGov.HasName(Control_IdParent)) phuemulBigDataGov.logMessageWarn(s"HuemulControl: FINISH ALL OK")
    phuemulBigDataGov.logMessageWarn(s"HuemulControl: FINISH processName: ${Control_ClassName}, ProcessExec_Id: ${Control_Id}, Time Elapsed: ${DiffDate.hour + (DiffDate.days*24)}:${DiffDate.minute}:${DiffDate.second} ")

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
    
    if (Control_IdParent == null) phuemulBigDataGov.logMessageWarn(s"HuemulControl: FINISH ERROR")
    phuemulBigDataGov.logMessageWarn(s"HuemulControl: FINISH processName: ${Control_ClassName}, ProcessExec_Id: ${Control_Id}, Time Elapsed: ${DiffDate.hour + (DiffDate.days*24)}:${DiffDate.minute}:${DiffDate.second} ")

      
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
                           ,Control_Error.ControlError_LineNumber.toString()
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
                             ,LineNumber.toString()
                             ,methodName
                             ,"" //--as Error_Detail
                             ,WhoWriteError
                           )
    }
        
                               
  }
  
  
  def NewStep(StepName: String) {
    phuemulBigDataGov.logMessageWarn(s"HuemulControl: Step: $StepName")
    
   
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
    phuemulBigDataGov.logMessageWarn(s"HuemulControl: TestPlan ${if (p_testPlan_IsOK) "OK " else "ERROR " }: testPlan_name: ${p_testPlan_name}, resultExpected: ${p_testPlan_resultExpected}, resultReal: ${p_testPlan_resultReal} ")
    
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
    
    return testPlan_Id
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
    
      val TotalProcess = ResultTestPlan.GetValue("cantidad",ResultTestPlan.ResultSet(0)).toString().toInt //ResultTestPlan.ResultSet(0).getAs[Long]("cantidad".toLowerCase()).toInt
      var TotalOK = ResultTestPlan.GetValue("total_ok",ResultTestPlan.ResultSet(0)).toString().toInt //ResultTestPlan.ResultSet(0).getAs[Long]("total_ok".toLowerCase()).toInt
      if (TotalOK == null)
        TotalOK = 0
         
      if (TotalProcess != TotalOK) {
        phuemulBigDataGov.logMessageWarn(s"HuemulControl: TestPlan_IsOkById with Error: Total Process: $TotalProcess, Total OK: $TotalOK, Total Error: ${TotalProcess-TotalOK}, Total Process Expected: $TotalProcessExpected")
      } else if (TotalProcess != TotalProcessExpected) {
        phuemulBigDataGov.logMessageWarn(s"HuemulControl: TestPlan_IsOkById doesn't have the expected process: Total Process: $TotalProcess, Total OK: $TotalOK, Total Error: ${TotalProcess-TotalOK}, Total Process Expected: $TotalProcessExpected")
      } else
        phuemulBigDataGov.logMessageWarn(s"HuemulControl: TestPlan_IsOkById OK: Total Process: $TotalProcess, Total OK: $TotalOK, Total Error: ${TotalProcess-TotalOK}, Total Process Expected: $TotalProcessExpected")
        IsOK = true
    }
    
    return IsOK
  }
  
  /**
   * TestPlan_CurrentIsOK: Determine if current class Testplan finish OK. for a specific Id use TestPlan_IsOkById
   * @author sebas_rod
   * @param TotalOkExpected (default null): indicate total number of TestPlan expected ok. Null exptected all OK & > 1 testplan
   * Version >= 1.4
   */
  def TestPlan_CurrentIsOK(TotalOkExpected: Integer = null): Boolean = {
    var IsOK: Boolean = false
    
    val NumOK = getTestPlanDetails.count { x => x.gettestPlan_IsOK == true }
    val NumTotal = getTestPlanDetails.length
    
    if (TotalOkExpected == null) {
      
     
      if (NumTotal <= 0)
        IsOK = false
      else if (NumTotal == NumOK)
        IsOK = true
    
      RegisterTestPlan(_testPlanGroup_Id, "TestPlan_IsOK", "FINAL CHECK", "All test plan OK && TestPlan > 0", s"N° Total ($NumTotal) = N° OK ($NumOK)",IsOK)
      phuemulBigDataGov.logMessageWarn(s"HuemulControl: TestPlan FINISH: ${if (IsOK) "OK " else "ERROR " }: N° Total: ${NumTotal}, N° OK: ${NumOK}, N° Error: ${NumTotal - NumOK} ")
    } else {
       if (NumOK == TotalOkExpected)
         IsOK = true
         
       
       RegisterTestPlan(_testPlanGroup_Id, "TestPlan_IsOK", "FINAL CHECK", "User Expected OK = TestPlan OK", s"N° Total ($NumTotal) = N° OK Expected ($TotalOkExpected)",IsOK)
       phuemulBigDataGov.logMessageWarn(s"HuemulControl: TestPlan FINISH: ${if (IsOK) "OK " else "ERROR " }: N° Total: ${NumTotal}, N° OK: ${NumOK}, N° Error: ${NumTotal - NumOK}, N° User Expected OK: ${TotalOkExpected} ")
    }
    
    
    return IsOK
  }
  
  
  def RegisterDQuality (Table_Name: String
                             , BBDD_Name: String
                             , DF_Alias: String
                             , ColumnName: String
                             , DQ_Name: String
                             , DQ_Description: String
                             , DQ_QueryLevel: huemulType_DQQueryLevel //DQ_IsAggregate: Boolean
                             , DQ_Notification: huemulType_DQNotification ////DQ_RaiseError: Boolean
                             , DQ_SQLFormula: String
                             , DQ_toleranceError_Rows: java.lang.Long
                             , DQ_toleranceError_Percent: Decimal
                             , DQ_ResultDQ: String
                             , DQ_ErrorCode: Integer
                             , DQ_NumRowsOK: java.lang.Long
                             , DQ_NumRowsError: java.lang.Long
                             , DQ_NumRowsTotal: java.lang.Long
                             , DQ_IsError: Boolean) {
                
    //Create New Id
    val DQId = huemulBigDataGov.huemul_GetUniqueId()

    if (huemulBigDataGov.RegisterInControl) {
      //Insert processExcec
      control_DQ_add(   DQId
                         , Table_Name
                         , BBDD_Name
                         , this.Control_ClassName
                         , this.Control_Id
                         , ColumnName
                         , DF_Alias
                         , DQ_Name
                         , DQ_Description
                         , DQ_QueryLevel.toString()
                         , DQ_Notification.toString()
                         , DQ_SQLFormula
                         , DQ_toleranceError_Rows
                         , DQ_toleranceError_Percent
                         , DQ_ResultDQ
                         , DQ_ErrorCode
                         , DQ_NumRowsOK
                         , DQ_NumRowsError
                         , DQ_NumRowsTotal
                         , DQ_IsError
                         , Control_ClassName
                         )
     
    }
    
  }
  
  def GetCharRepresentation(value: String): String  = {
    return if (value == "\t") "TAB"
    else value
           
  }
  
  def RegisterRAW_USE(dapi_raw: huemul_DataLake) {        
    dapi_raw.setrawFiles_id(huemulBigDataGov.huemul_GetUniqueId())
    
    if (huemulBigDataGov.RegisterInControl) {
      //Insert processExcec
      val ExecResult = control_rawFiles_add(dapi_raw.getrawFiles_id
                         , dapi_raw.LogicalName
                         , dapi_raw.GroupName.toUpperCase()
                         , dapi_raw.Description
                         , dapi_raw.SettingInUse.ContactName
                         , dapi_raw.getFrequency.toString()
                         , Control_ClassName
                         )
     
      //dapi_raw.getrawFiles_id if new, otherwise take Id from DataBase                   
      val LocalRawFiles_id = ExecResult.OpenVar
      
      //Insert Config Details
      dapi_raw.SettingByDate.foreach { x => 
        //Insert processExcec
        var RAWFilesDet_id = huemulBigDataGov.huemul_GetUniqueId()
        
        val ExecResultDet = control_RAWFilesDet_add(LocalRawFiles_id
                             ,RAWFilesDet_id
                             ,x.StartDate
                             ,x.EndDate
                             ,x.FileName
                             ,x.LocalPath
                             ,x.GetPath(x.GlobalPath)
                             ,x.DataSchemaConf.ColSeparatorType.toString()
                             ,GetCharRepresentation(x.DataSchemaConf.ColSeparator)
                             ,""  //rawfilesdet_data_headcolstring
                             ,x.LogSchemaConf.ColSeparatorType.toString()
                             ,GetCharRepresentation(x.LogSchemaConf.ColSeparator) 
                             ,""  //rawfilesdet_log_headcolstring
                             ,x.LogNumRows_FieldName 
                             ,x.ContactName 
                             ,Control_ClassName
                             )
            
        RAWFilesDet_id = ExecResultDet.OpenVar //if new take RAWFilesDet_id, if update take from DataBase
         
        if (x.DataSchemaConf.ColumnsDef != null) {
           var pos: Integer = 0
           x.DataSchemaConf.ColumnsDef.foreach { y =>
             control_RAWFilesDetFields_add(LocalRawFiles_id
                                          ,RAWFilesDet_id
                                          ,y.getcolumnName_TI
                                          ,y.getcolumnName_Business
                                          ,y.getDescription
                                          ,y.getDataType.toString()
                                          ,pos
                                          ,y.getPosIni
                                          ,y.getPosFin
                                          ,y.getApplyTrim     
                                          ,y.getConvertToNull   
                                          ,Control_ClassName
                                          )
                pos += 1
             }
         }
      }
    
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
                             ,dapi_raw.DataFramehuemul.getNumRows.toString()
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
                             ,DefMaster.GetCurrentDataBase()  
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
                             ,Control_ClassName
                                  )
     
    }
  }

  def RegisterMASTER_CREATE_Basic(DefMaster: huemul_Table) {
    if (huemulBigDataGov.IsTableRegistered(DefMaster.TableName))
      return
      
    if (huemulBigDataGov.DebugMode) phuemulBigDataGov.logMessageDebug(s"HuemulControl:    Register Table&Columns in Control")
    var LocalNewTable_id = huemulBigDataGov.huemul_GetUniqueId()

    if (huemulBigDataGov.RegisterInControl) {
      //Table
      val ExecResultTable = control_Tables_addOrUpd(LocalNewTable_id
                             ,DefMaster.GetCurrentDataBase()
                             ,DefMaster.TableName
                             ,DefMaster.getDescription
                             ,DefMaster.getBusiness_ResponsibleName
                             ,DefMaster.getIT_ResponsibleName
                             ,DefMaster.getPartitionField
                             ,DefMaster.getTableType.toString()
                             ,DefMaster.getStorageType.toString()
                             ,DefMaster.getLocalPath
                             ,DefMaster.GetPath(DefMaster.getGlobalPaths)
                             ,"" //--as Table_SQLCreate
                             ,DefMaster.getFrequency.toString()
                             ,Control_ClassName
                             )
      
      LocalNewTable_id = ExecResultTable.OpenVar
      DefMaster._setAutoIncUpate(ExecResultTable.OpenVar2.toLong)
      
      //Insert control_Columns
      var i: Integer = 0
      var localDatabaseName = DefMaster.GetCurrentDataBase()
      DefMaster.GetColumns().foreach { x => 
        val Column_Id = huemulBigDataGov.huemul_GetUniqueId()
    
        control_Columns_addOrUpd( LocalNewTable_id
                                 ,Column_Id
                                 ,i
                                 ,x.get_MyName()
                                 ,x.Description
                                 ,null //--as Column_Formula
                                 ,x.DataType.sql
                                 ,false //--as Column_SensibleData
                                 ,x.getMDM_EnableDTLog
                                 ,x.getMDM_EnableOldValue
                                 ,x.getMDM_EnableProcessLog
                                 ,x.getDefaultValue
                                 ,x.getSecurityLevel.toString()
                                 ,x.getEncryptedType
                                 ,x.getARCO_Data
                                 ,x.getNullable
                                 ,x.getIsPK
                                 ,x.getIsUnique
                                 ,x.getDQ_MinLen
                                 ,x.getDQ_MaxLen
                                 ,x.getDQ_MinDecimalValue
                                 ,x.getDQ_MaxDecimalValue
                                 ,x.getDQ_MinDateTimeValue
                                 ,x.getDQ_MaxDateTimeValue
                                 ,x.getDQ_RegExp
                                 ,Control_ClassName
                                 )     
          i += 1
        }
      
      //Insert control_tablesrel_add
      DefMaster.GetForeingKey().foreach { x =>       
        val p_tablerel_id = huemulBigDataGov.huemul_GetUniqueId()
        val InstanceTable = x._Class_TableName.asInstanceOf[huemul_Table]
        val localDatabaseName = InstanceTable.GetCurrentDataBase()
        
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
                                     ,y.PK.get_MyName()
                                     ,LocalNewTable_id
                                     ,y.FK.get_MyName() 
                                     ,Control_ClassName)
          }
      }
    }
  }
  
  def RegisterMASTER_CREATE_Use(DefMaster: huemul_Table) {     
      
    if (huemulBigDataGov.RegisterInControl) {
      //Insert control_TablesUse
      DefMaster._tablesUseId = LocalIdStep
      
       control_TablesUse_add(  DefMaster.TableName
                             ,DefMaster.GetCurrentDataBase()  
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
                             ,false //--as TableUse_Read
                             ,true //--as TableUse_Write
                             ,DefMaster.NumRows_New()
                             ,DefMaster.NumRows_Update()
                             ,DefMaster.NumRows_Updatable()
                             ,DefMaster.NumRows_NoChange() 
                             ,DefMaster.NumRows_Delete()
                             ,DefMaster.NumRows_Total()
                             ,DefMaster.getPartitionValue
                             ,Control_ClassName
                            ) 
    }
                        
  }
 
  
  def RaiseError(txt: String) {
    sys.error(txt)
  }
  
  private def ReplaceSQLStringNulls(ColumnName: String): String = {
    return huemulBigDataGov.ReplaceSQLStringNulls(ColumnName)
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
      VALUES( ${ReplaceSQLStringNulls(p_testPlan_Id)}
           , ${ReplaceSQLStringNulls(p_testPlanGroup_Id)}
           , ${ReplaceSQLStringNulls(p_processExec_id)}
           , ${ReplaceSQLStringNulls(p_process_id)}
           , ${ReplaceSQLStringNulls(p_testPlan_name)}
           , ${ReplaceSQLStringNulls(p_testPlan_description)}
           , ${ReplaceSQLStringNulls(p_testPlan_resultExpected)}
           , ${ReplaceSQLStringNulls(p_testPlan_resultReal)}
           , ${if (p_testPlan_IsOK) "1" else "0"}
           , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
           , ${ReplaceSQLStringNulls(p_Executor_Name)}
      )
    """)
           
    return ExecResult
                       
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
      VALUES( ${ReplaceSQLStringNulls(p_Feature_Id)}
           , ${ReplaceSQLStringNulls(p_testPlan_Id)}
           , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
           , ${ReplaceSQLStringNulls(p_Executor_Name)}
      )
    """)
           
    return ExecResult             
  }
  
  /**
   * 
   */
  private def control_TestPlanTest_GetOK ( p_testPlan_Id: String): huemul_JDBCResult =  {
    var ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
                    select cast(count(1) as Integer) as cantidad, cast(sum(testplan_isok) as Integer) as total_ok 
                    from control_testplan 
                    where testplangroup_id = '${p_testPlan_Id}' 
                    and testplan_name = 'TestPlan_IsOK'
          """)
  
    return ExecResult             
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
                    WHERE process_id = ${ReplaceSQLStringNulls(p_process_id)}
          """)

    if (ExecResult.ResultSet.length == 1) {
      //Record exist, update
      
      //ExecResult.ResultSet(0).schema.printTreeString()
      
      //val MDM_ManualChange =  ExecResult.ResultSet(0).getAs[Long]("mdm_manualchange".toLowerCase()).asInstanceOf[Int]
      val MDM_ManualChange = ExecResult.GetValue("mdm_manualchange".toLowerCase(), ExecResult.ResultSet(0)).toString().toInt
      if (MDM_ManualChange == 0) {
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
                      UPDATE control_process 
                      SET  process_name			    = ${ReplaceSQLStringNulls(p_process_name)}
                         , process_filename     = ${ReplaceSQLStringNulls(p_process_FileName)}
                    	   , process_description	= ${ReplaceSQLStringNulls(p_process_description)}
                    	   , process_owner			  = ${ReplaceSQLStringNulls(p_process_owner)}
                    	   , process_frequency    = ${ReplaceSQLStringNulls(p_process_frequency)}
                    	WHERE process_id = ${ReplaceSQLStringNulls(p_process_id)}
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
            	VALUES( ${ReplaceSQLStringNulls(p_process_id)}
            		   , '0'
            		   , ${ReplaceSQLStringNulls(p_process_name)}
                   , ${ReplaceSQLStringNulls(p_process_FileName)}
            		   , ${ReplaceSQLStringNulls(p_process_description)}
            		   , ${ReplaceSQLStringNulls(p_process_owner)}
            		   , ${ReplaceSQLStringNulls(p_process_frequency)}
            		   , 0
            		   , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
            		   , ${ReplaceSQLStringNulls(p_mdm_processname)}
              )            		   
            """)
    }
    
    return ExecResult             
  }
  
  private def control_singleton_Add (   p_singleton_id: String
                       , p_application_Id: String
                       , p_singleton_name: String): huemul_JDBCResult =  {
    var ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
              SELECT application_id
              FROM control_singleton
              WHERE singleton_id = ${ReplaceSQLStringNulls(p_singleton_id)}
    """)
    var application_Id: String = ""
    
    if (ExecResult.ResultSet.length == 0) {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      insert into control_singleton (singleton_id
                								   , application_id
                								   , singleton_name
                								   , mdm_fhcreate) 	
  		VALUES( ${ReplaceSQLStringNulls(p_singleton_id)}
  			   , ${ReplaceSQLStringNulls(p_application_Id)}
  			   , ${ReplaceSQLStringNulls(p_singleton_name)}
  			   , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
      )  			   
      """)
      
      //If error, get application id
      if (ExecResult.IsError) {
        phuemulBigDataGov.logMessageError(s"${ExecResult.ErrorDescription}")
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
              SELECT *
              FROM control_singleton
              WHERE singleton_id = ${ReplaceSQLStringNulls(p_singleton_id)}
        """)
        
        if (ExecResult.ResultSet.length > 0)
          application_Id = ExecResult.ResultSet(0).getAs[String]("application_id".toLowerCase())
      }
      
    } else {
      application_Id = ExecResult.ResultSet(0).getAs[String]("application_id".toLowerCase())
    }
    
    return ExecResult             
  }
  
  private def control_singleton_remove (p_singleton_id: String): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      DELETE FROM control_singleton 
      WHERE singleton_id = ${ReplaceSQLStringNulls(p_singleton_id)}
    """)
           
    return ExecResult             
  }
  
  private def control_executors_remove (p_application_Id: String): huemul_JDBCResult =  {
    val ExecResult1 = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""DELETE FROM control_singleton WHERE application_id = ${ReplaceSQLStringNulls(p_application_Id)}""")
    val ExecResult2 = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""DELETE FROM control_executors WHERE application_id = ${ReplaceSQLStringNulls(p_application_Id)}""")
           
    return ExecResult2             
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
    	VALUES( ${ReplaceSQLStringNulls(p_application_Id)}
    		   , ${ReplaceSQLStringNulls(p_IdSparkPort)}
    		   , ${ReplaceSQLStringNulls(p_IdPortMonitoring)}
    		   , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
    		   , ${ReplaceSQLStringNulls(p_Executor_Name)}
      )    		   
    """)
           
    return ExecResult             
  }
  
  
  private def control_processExec_UpdParam (p_processExec_id: String
                                    ,p_paramName: String
                                    ,p_value: Integer
      ): huemul_JDBCResult =  {
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexec 
	    SET processexec_param_$p_paramName = ${p_value}
      WHERE processexec_id = ${ReplaceSQLStringNulls(p_processExec_id)} 
    	
    """)
           
    return ExecResult             
  }
  
  private def control_processExec_UpdParamInfo (p_processExec_id: String
                                    ,p_paramName: String
                                    ,p_value: String
      ): huemul_JDBCResult =  {
    
    processExec_param_others = s"${processExec_param_others}${if (processExec_param_others == "") "" else ", "}{${p_paramName}}=${p_value}" 
    
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexec 
	    SET processexec_param_others = ${ReplaceSQLStringNulls(processExec_param_others)} 
      WHERE processexec_id = ${ReplaceSQLStringNulls(p_processExec_id)} 
    	
    """)
           
    return ExecResult             
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
                              ,error_id
                              ,mdm_fhcreate
                              ,mdm_processname)
         VALUES( ${ReplaceSQLStringNulls(p_processExec_id)}
    			  ,${ReplaceSQLStringNulls(p_processExec_idParent)}
    			  ,${ReplaceSQLStringNulls(p_process_id)}
    			  ,${ReplaceSQLStringNulls(p_Malla_id)}
    			  ,${ReplaceSQLStringNulls(p_application_Id)}
    			  ,1
    			  ,0
    			  ,0
    			  ,0
    			  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
    			  ,null
    			  ,0
    			  ,0
    			  ,0
    			  ,${ReplaceSQLStringNulls(p_processExec_WhosRun)}
    			  ,${if (p_processExec_DebugMode) "1" else "0"}
    			  ,${ReplaceSQLStringNulls(p_processExec_Environment)}
    			  ,${p_processExec_param_year}	
    			  ,${p_processExec_param_month}	
    			  ,${p_processExec_param_day}	
    			  ,${p_processExec_param_hour}	
    			  ,${p_processExec_param_min}	
    			  ,${p_processExec_param_sec}	
    			  ,''
    			  ,null
    			  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
    			  ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
      )    			  
    	
    """)
           
    return ExecResult             
  }
  
  
  private def control_processExec_Finish (p_processExec_id: String
                                    ,p_processExecStep_id: String
                                    ,p_error_id: String
      ): huemul_JDBCResult =  {
    val DiffDateStep = huemulBigDataGov.getDateTimeDiff(processExecStep_dtStart, processExec_dtEnd)
     
    
    var ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexecstep
      SET  processexecstep_status  = ${if (p_error_id == null || p_error_id == "") "'OK'" else "'ERROR'"}
    			,processexecstep_dtend   = ${ReplaceSQLStringNulls(huemulBigDataGov.dateTimeFormat.format(processExec_dtEnd.getTime()) )}
    			,processexecstep_durhour = ${DiffDateStep.hour + (DiffDateStep.days*24)}
    			,processexecstep_durmin  = ${DiffDateStep.minute}
    			,processexecstep_dursec  = ${DiffDateStep.second}
    			,error_id				         = ${ReplaceSQLStringNulls(p_error_id)}
	    WHERE processexecstep_id = ${ReplaceSQLStringNulls(p_processExecStep_id)}    	
    """)
    
    if (!ExecResult.IsError) {
      val DiffDate = huemulBigDataGov.getDateTimeDiff(processExec_dtStart, processExec_dtEnd)
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
      UPDATE control_processexec
        SET  processexec_iscancelled = 0
        	  ,processexec_isenderror = ${if (p_error_id == null || p_error_id == "") "0" else "1"}
        	  ,processexec_isendok	  = ${if (p_error_id == null || p_error_id == "") "1" else "0"}
        	  ,processexec_dtend	    = ${ReplaceSQLStringNulls(huemulBigDataGov.dateTimeFormat.format(processExec_dtEnd.getTime()) )}
        	  ,processexec_durhour	  = ${DiffDate.hour + (DiffDate.days*24)}
        	  ,processexec_durmin	    = ${DiffDate.minute}
        	  ,processexec_dursec	    = ${DiffDate.second}
        	  ,error_id				        = ${ReplaceSQLStringNulls(p_error_id)}
        	WHERE processexec_id = ${ReplaceSQLStringNulls(p_processExec_id )}  	
      """)
    }
           
    return ExecResult             
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
         ,processexecstep_dtend   = ${ReplaceSQLStringNulls(huemulBigDataGov.dateTimeFormat.format(endDataTime.getTime()) )}
  		   ,processexecstep_durhour = ${DiffDateStep.hour + (DiffDateStep.days*24)}
  		   ,processexecstep_durmin  = ${DiffDateStep.minute}
  		   ,processexecstep_dursec  = ${DiffDateStep.second}
  		WHERE processexecstep_id = ${ReplaceSQLStringNulls(p_processExecStep_idAnt)}	
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
       VALUES(  ${ReplaceSQLStringNulls(p_processExecStep_id)}	
      			  ,${ReplaceSQLStringNulls(p_processExec_id)}	
      			  ,${ReplaceSQLStringNulls(p_processExecStep_Name)}	
      			  ,'RUNNING'
      			  ,${ReplaceSQLStringNulls(huemulBigDataGov.dateTimeFormat.format(processExecStep_dtStart.getTime()) )}
      			  ,null
      			  ,0
      			  ,0
      			  ,0
      			  ,null
      			  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
      			  ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}	
      )
      """)
           
    return ExecResult             
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
                             ,p_DQ_NumRowsOK: java.lang.Long
                             ,p_DQ_NumRowsError: java.lang.Long
                             ,p_DQ_NumRowsTotal: java.lang.Long
                             ,p_DQ_IsError: Boolean
                             ,p_MDM_ProcessName: String
                             
      ): huemul_JDBCResult =  {
    
    //Get Table Id
    val ExecResultTable = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select table_id 
          from control_tables 
          where table_bbddname = ${ReplaceSQLStringNulls(p_BBDD_name)} 
          and   table_name     = ${ReplaceSQLStringNulls(p_Table_name)}	
      """)
    val LocalTable_Id: String = if (ExecResultTable.IsError || ExecResultTable.ResultSet.length == 0) null else ExecResultTable.ResultSet(0).getAs[String]("table_id".toLowerCase()) 
      
    //Get column Id
    val ExecResultColumn = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          select column_id 
          from control_columns 
          where table_id     = ${ReplaceSQLStringNulls(LocalTable_Id)} 
          and   column_name  = ${ReplaceSQLStringNulls(p_Column_Name)}	
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
                                 ,dq_numrowsok
                                 ,dq_numrowserror
                                 ,dq_numrowstotal
                                 ,dq_iserror
                                 ,mdm_fhcreate
                                 ,mdm_processname)
      	VALUES(  ${ReplaceSQLStringNulls(p_DQ_Id)}
        			 ,${ReplaceSQLStringNulls(LocalTable_Id)}
        			 ,${ReplaceSQLStringNulls(p_Process_Id)}
        			 ,${ReplaceSQLStringNulls(p_ProcessExec_Id)}
        			 ,${ReplaceSQLStringNulls(LocalColumn_Id)}
        			 ,${ReplaceSQLStringNulls(p_Column_Name)}
        			 ,${ReplaceSQLStringNulls(p_Dq_AliasDF)}
        			 ,${ReplaceSQLStringNulls(p_DQ_Name)}
        			 ,${ReplaceSQLStringNulls(p_DQ_Description)}
        			 ,${ReplaceSQLStringNulls(p_DQ_QueryLevel)}
        			 ,${ReplaceSQLStringNulls(p_DQ_Notification)}
        			 ,${ReplaceSQLStringNulls(p_DQ_SQLFormula)}
        			 ,${p_DQ_toleranceError_Rows}
        			 ,${p_DQ_toleranceError_Percent}
        			 ,${ReplaceSQLStringNulls(p_DQ_ResultDQ)}
               ,${p_DQ_ErrorCode}
        			 ,${p_DQ_NumRowsOK}
        			 ,${p_DQ_NumRowsError}
        			 ,${p_DQ_NumRowsTotal}
               ,${if (p_DQ_IsError) "1" else "0"}
        			 ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
        			 ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
      )        			 
      """)
           
    return ExecResult             
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
          where rawfiles_groupname = ${ReplaceSQLStringNulls(p_RAWFiles_GroupName)} 
          and   rawfiles_logicalname = ${ReplaceSQLStringNulls(p_RAWFiles_LogicalName)} 
      """)
      
    var LocalMDM_ManualChange: Int = -1
    var Localrawfiles_id: String = null
    if (!ExecResultRawFiles.IsError && ExecResultRawFiles.ResultSet.length == 1) {
      LocalMDM_ManualChange = ExecResultRawFiles.GetValue("mdm_manualchange",ExecResultRawFiles.ResultSet(0)).toString().toInt
      Localrawfiles_id = ExecResultRawFiles.ResultSet(0).getAs[String]("rawfiles_id".toLowerCase())
      
    }
    
    var ExecResult: huemul_JDBCResult = null
    if (ExecResultRawFiles.ResultSet.length == 1 && LocalMDM_ManualChange == 0) {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_rawfiles
          SET rawfiles_description	= ${ReplaceSQLStringNulls(p_RAWFiles_Description)}
        		 ,rawfiles_owner		    = ${ReplaceSQLStringNulls(p_RAWFiles_Owner)}
        		 ,rawfiles_frequency	  = ${ReplaceSQLStringNulls(p_RAWFiles_frequency)}
        	WHERE rawfiles_id = ${ReplaceSQLStringNulls(Localrawfiles_id)}
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
        	VALUES( ${ReplaceSQLStringNulls(p_RAWFiles_id)}
        			 ,'0'
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_LogicalName)}
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_GroupName)}
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_Description)}
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_Owner)}
        			 ,${ReplaceSQLStringNulls(p_RAWFiles_frequency)}
        			 ,0 
        			 ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
        			 ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
      )        			 
        """)
        
      ExecResult.OpenVar = p_RAWFiles_id
    }
           
    return ExecResult             
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
					WHERE rawfiles_id = ${ReplaceSQLStringNulls(p_RAWFiles_Id)}
					AND   rawfilesdet_startdate = ${ReplaceSQLStringNulls(p_RAWFilesDet_StartDate_string)}
      """)
      
    var Localrawfilesdet_id: String = null
    if (!ExecResultRawFilesDet.IsError && ExecResultRawFilesDet.ResultSet.length == 1) {
      Localrawfilesdet_id = ExecResultRawFilesDet.ResultSet(0).getAs[String]("rawfilesdet_id".toLowerCase())
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          update control_rawfilesdetfields 
          set mdm_active = 0 
          where rawfilesdet_id = 	${ReplaceSQLStringNulls(Localrawfilesdet_id)}
        """)
        
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_rawfilesdet
      		SET rawfilesdet_enddate                   = ${ReplaceSQLStringNulls(p_RAWFilesDet_EndDate_string)}						
      		   ,rawfilesdet_filename					        = ${ReplaceSQLStringNulls(p_RAWFilesDet_FileName)}					
      		   ,rawfilesdet_localpath					        = ${ReplaceSQLStringNulls(p_RAWFilesDet_LocalPath)}					
      		   ,rawfilesdet_globalpath					      = ${ReplaceSQLStringNulls(p_RAWFilesDet_GlobalPath)}					
      		   ,rawfilesdet_data_colseptype		  = ${ReplaceSQLStringNulls(p_rawfilesdet_data_colseptype)}		
      		   ,rawfilesdet_data_colsep			    = ${ReplaceSQLStringNulls(p_rawfilesdet_data_colsep)}			
      		   ,rawfilesdet_data_headcolstring	= ${ReplaceSQLStringNulls(p_rawfilesdet_data_headcolstring)}	
      		   ,rawfilesdet_log_colseptype		  = ${ReplaceSQLStringNulls(p_rawfilesdet_log_colseptype)}		
      		   ,rawfilesdet_log_colsep			    = ${ReplaceSQLStringNulls(p_rawfilesdet_log_colsep)}			
      		   ,rawfilesdet_log_headcolstring		= ${ReplaceSQLStringNulls(p_rawfilesdet_log_headcolstring)}		
      		   ,rawfilesdet_log_numrowsfield		  = ${ReplaceSQLStringNulls(p_rawfilesdet_log_numrowsfield)}		
      		   ,rawfilesdet_contactname					      = ${ReplaceSQLStringNulls(p_RAWFilesDet_ContactName)}					
      		WHERE rawfilesdet_id = ${ReplaceSQLStringNulls(Localrawfilesdet_id)}
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
        		VALUES(  ${ReplaceSQLStringNulls(p_RAWFilesDet_id)}
          			   ,${ReplaceSQLStringNulls(p_RAWFiles_Id)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_StartDate_string)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_EndDate_string)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_FileName)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_LocalPath)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_GlobalPath)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_data_colseptype)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_data_colsep)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_data_headcolstring)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_log_colseptype)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_log_colsep)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_log_headcolstring)}
          			   ,${ReplaceSQLStringNulls(p_rawfilesdet_log_numrowsfield)}
          			   ,${ReplaceSQLStringNulls(p_RAWFilesDet_ContactName)}
          			   ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
          			   ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
      )          			   
        """)
      ExecResult.OpenVar = p_RAWFilesDet_id
    }
    
    
    return ExecResult             
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
					WHERE rawfilesdet_id = ${ReplaceSQLStringNulls(p_RAWFilesDet_id)}
					AND   rawfilesdetfields_logicalname = ${ReplaceSQLStringNulls(p_RAWFilesDetFields_LogicalName)}
      """)
      
    
    if (!ExecResultRawFilesDetFields.IsError && ExecResultRawFilesDetFields.ResultSet.length == 1) {
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_rawfilesdetfields
        	SET  mdm_active = 1
        		,rawfilesdetfields_itname		    =  ${ReplaceSQLStringNulls(p_RAWFilesDetFields_ITName)}
        		,rawfilesdetfields_logicalname  =  ${ReplaceSQLStringNulls(p_RAWFilesDetFields_LogicalName)}
        		,rawfilesdetfields_description  =  ${ReplaceSQLStringNulls(p_RAWFilesDetFields_description)}
        		,rawfilesdetfields_datatype     =  ${ReplaceSQLStringNulls(p_RAWFilesDetFields_DataType)}
        		,rawfilesdetfields_position     =  ${P_RAWFilesDetFields_Position}
        		,rawfilesdetfields_posini       =  ${P_RAWFilesDetFields_PosIni}
        		,rawfilesdetfields_posfin       =  ${P_RAWFilesDetFields_PosFin}
        		,rawfilesdetfields_applytrim    =  ${if (p_RAWFilesDetFields_ApplyTrim) "1" else "0"}
        		,rawfilesdetfields_convertnull  =  ${if (p_RAWFilesDetFields_ConvertNull) "1" else "0"}
        	WHERE rawfilesdet_id = ${ReplaceSQLStringNulls(p_RAWFilesDet_id)}
        	AND   rawfilesdetfields_logicalname = ${ReplaceSQLStringNulls(p_RAWFilesDetFields_LogicalName)}
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
        	VALUES( ${ReplaceSQLStringNulls(p_RAWFilesDet_id)}
          			,${ReplaceSQLStringNulls(p_RAWFilesDetFields_ITName)}
          			,${ReplaceSQLStringNulls(p_RAWFilesDetFields_LogicalName)}
          			,${ReplaceSQLStringNulls(p_RAWFilesDetFields_description)}
          			,${ReplaceSQLStringNulls(p_RAWFilesDetFields_DataType)}
          			,${P_RAWFilesDetFields_Position}
          			,${P_RAWFilesDetFields_PosIni}
          			,${P_RAWFilesDetFields_PosFin}
          			,${if (p_RAWFilesDetFields_ApplyTrim) "1" else "0"}
          			,${if (p_RAWFilesDetFields_ConvertNull) "1" else "0"} 
          			,1
          			,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
          			,${ReplaceSQLStringNulls(P_MDM_ProcessName)}
      )          			
        """)
    }
    
    return ExecResult             
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
        	VALUES(  ${ReplaceSQLStringNulls(p_RAWFilesUse_id)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_Id)}
          			 ,${ReplaceSQLStringNulls(p_Process_Id)}
          			 ,${ReplaceSQLStringNulls(p_ProcessExec_Id)}
                 ,${p_RAWFilesUse_Year}
                 ,${p_RAWFilesUse_Month}   
                 ,${p_RAWFilesUse_Day}     
                 ,${p_RAWFilesUse_Hour}    
                 ,${p_RAWFilesUse_Minute}   
                 ,${p_RAWFilesUse_Second}  
                 ,${ReplaceSQLStringNulls(p_RAWFilesUse_params)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_fullName)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_fullPath)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_numRows)}
          			 ,${ReplaceSQLStringNulls(p_RAWFiles_HeaderLine)}
          			 ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
          			 ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
      )          			 
        """)
    
    
    return ExecResult             
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
        	VALUES( ${ReplaceSQLStringNulls(p_processExec_id)}
          		  ,${ReplaceSQLStringNulls(p_processExecParams_Name)}
          		  ,${ReplaceSQLStringNulls(p_processExecParams_Value)}
          		  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
          		  ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
      )          		  
        """)
    
    
    return ExecResult             
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
                             ,p_TableUse_numRowsTotal: java.lang.Long
                             ,p_TableUse_PartitionValue: String
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    
     //Get ExecResultRawFiles Id
    val ExecResult_TableId = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT table_id
					FROM control_tables 
					WHERE table_name = ${ReplaceSQLStringNulls(p_Table_Name)}
          and   table_bbddname = ${ReplaceSQLStringNulls(p_Table_BBDDName)}
      """)
    
    var LocalTable_id: String = null
    if (!ExecResult_TableId.IsError && ExecResult_TableId.ResultSet.length == 1){
      LocalTable_id = ExecResult_TableId.ResultSet(0).getAs[String]("table_id".toLowerCase())
    }
      
      
    val ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          insert into control_tablesuse ( table_id
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
                      								   ,tableuse_numrowstotal
                      								   ,tableuse_partitionvalue
                      								   ,mdm_fhcreate
                      								   ,mdm_processname)
        	VALUES(  ${ReplaceSQLStringNulls(LocalTable_id)}
          		   ,${ReplaceSQLStringNulls(p_Process_Id)}
          		   ,${ReplaceSQLStringNulls(p_ProcessExec_Id)}
                 ,${ReplaceSQLStringNulls(p_ProcessExecStep_Id)}
                 ,${p_TableUse_Year}
                 ,${p_TableUse_Month}
                 ,${p_TableUse_Day}
                 ,${p_TableUse_Hour}
                 ,${p_TableUse_Minute}
                 ,${p_TableUse_Second}
                 ,${ReplaceSQLStringNulls(p_TableUse_params)}
          		   ,${if (p_TableUse_Read) "1" else "0"}
          		   ,${if (p_TableUse_Write) "1" else "0"}
          		   ,${p_TableUse_numRowsNew}
          		   ,${p_TableUse_numRowsUpdate}
          		   ,${p_TableUse_numRowsUpdatable}
          		   ,${p_TableUse_numRowsNoChange}
          		   ,${p_TableUse_numRowsMarkDelete}
          		   ,${p_TableUse_numRowsTotal}
          		   ,${ReplaceSQLStringNulls(p_TableUse_PartitionValue)}
          		   ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
          		   ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
      )          		   
        """)
    
    
    return ExecResult             
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
                             ,p_Table_SQLCreate: String
                             ,p_Table_Frequency: String
                             ,p_MDM_ProcessName: String
      ): huemul_JDBCResult =  {
    var table_autoIncUpdate: Integer = 0
     //Get ExecResultRawFiles Id
    val ExecResult_TableId = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT table_id, table_autoincupdate
					FROM control_tables 
					WHERE table_name = ${ReplaceSQLStringNulls(p_Table_Name)}
          and   table_bbddname = ${ReplaceSQLStringNulls(p_Table_BBDDName)}
      """)
    
    var ExecResult: huemul_JDBCResult = null
    
    var LocalTable_id: String = null
    if (!ExecResult_TableId.IsError && ExecResult_TableId.ResultSet.length == 1){
      LocalTable_id = ExecResult_TableId.ResultSet(0).getAs[String]("table_id".toLowerCase())
      table_autoIncUpdate = ExecResult_TableId.ResultSet(0).getAs[Int]("table_autoIncUpdate".toLowerCase()) + 1
      
      val ExecResultCol = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_columns 
          SET mdm_active = 0 
          WHERE table_id = ${ReplaceSQLStringNulls(LocalTable_id)}
          """)
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_tables 
          SET table_description     = CASE WHEN mdm_manualchange = 1 THEN table_description		ELSE ${ReplaceSQLStringNulls(p_Table_Description)}	END	 
        		 ,table_businessowner	  = CASE WHEN mdm_manualchange = 1 THEN table_businessowner	ELSE ${ReplaceSQLStringNulls(p_Table_BusinessOwner)}	END
        		 ,table_itowner			    = CASE WHEN mdm_manualchange = 1 THEN table_itowner			  ELSE ${ReplaceSQLStringNulls(p_Table_ITOwner)}		END
        		 ,table_partitionfield	= ${ReplaceSQLStringNulls(p_Table_PartitionField)}	
        		 ,table_tabletype		    = ${ReplaceSQLStringNulls(p_Table_TableType)}		
        		 ,table_storagetype		  = ${ReplaceSQLStringNulls(p_Table_StorageType)}		
        		 ,table_localpath		    = ${ReplaceSQLStringNulls(p_Table_LocalPath)}		
        		 ,table_globalpath		  = ${ReplaceSQLStringNulls(p_Table_GlobalPath)}		
        		 ,table_sqlcreate    		= ${ReplaceSQLStringNulls(p_Table_SQLCreate)}		
        		 ,table_frequency		    = ${ReplaceSQLStringNulls(p_Table_Frequency)}
        		 ,table_autoincupdate   = ${ReplaceSQLStringNulls(table_autoIncUpdate.toString())}
          WHERE table_id = ${ReplaceSQLStringNulls(LocalTable_id)}
          """)
          
          
      ExecResult.OpenVar = LocalTable_id
      ExecResult.OpenVar2 = table_autoIncUpdate.toString()
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
                      								,table_sqlcreate
                      								,table_frequency
                                      ,table_autoincupdate
                      								,mdm_fhcreate
                      								,mdm_processname) 	
        	VALUES(   ${ReplaceSQLStringNulls(p_Table_id)}
            			,'0'
            			,${ReplaceSQLStringNulls(p_Table_BBDDName)}
            			,${ReplaceSQLStringNulls(p_Table_Name)}
            			,${ReplaceSQLStringNulls(p_Table_Description)}
            			,${ReplaceSQLStringNulls(p_Table_BusinessOwner)}
            			,${ReplaceSQLStringNulls(p_Table_ITOwner)}
            			,${ReplaceSQLStringNulls(p_Table_PartitionField)}
            			,${ReplaceSQLStringNulls(p_Table_TableType)}
            			,${ReplaceSQLStringNulls(p_Table_StorageType)}
            			,${ReplaceSQLStringNulls(p_Table_LocalPath)}
            			,${ReplaceSQLStringNulls(p_Table_GlobalPath)}
            			,${ReplaceSQLStringNulls(p_Table_SQLCreate)}
            			,${ReplaceSQLStringNulls(p_Table_Frequency)}
            			,1
            			,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
            			,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
      )            			
          """)
      ExecResult.OpenVar = p_Table_id
      ExecResult.OpenVar2 = "1"
    }
    
    
    return ExecResult             
  }
  
  
  private def control_Columns_addOrUpd (p_Table_id: String
                             ,p_Column_id: String
                             ,p_Column_Position: Integer
                             ,p_Column_Name: String
                             ,p_Column_Description: String
                             ,p_Column_Formula: String
                             ,p_Column_DataType: String
                             ,p_Column_SensibleData: Boolean
                             ,p_Column_EnableDTLog: Boolean
                             ,p_Column_EnableOldValue: Boolean
                             ,p_Column_EnableProcessLog: Boolean
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
                             ,p_MDM_ProcessName: String

                             
      ): huemul_JDBCResult =  {
    
     //Get Column Id
    val ExecResult_ColumnId = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT column_id
					FROM control_columns 
					WHERE table_id = ${ReplaceSQLStringNulls(p_Table_id)}
          and   column_name = ${ReplaceSQLStringNulls(p_Column_Name)}
      """)
    
    var ExecResult: huemul_JDBCResult = null
    var Localcolumn_id: String = null
    if (!ExecResult_ColumnId.IsError && ExecResult_ColumnId.ResultSet.length == 1){
      Localcolumn_id = ExecResult_ColumnId.ResultSet(0).getAs[String]("column_id".toLowerCase())
      
      ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_columns 
          SET column_position	            = ${p_Column_Position}				
        		 ,column_description			    = CASE WHEN mdm_manualchange = 1 THEN column_description ELSE ${ReplaceSQLStringNulls(p_Column_Description)}	END
        		 ,column_formula				      = CASE WHEN mdm_manualchange = 1 THEN column_formula ELSE ${ReplaceSQLStringNulls(p_Column_Formula)}	END			
        		 ,column_datatype				      = ${ReplaceSQLStringNulls(p_Column_DataType)}				
        		 ,column_sensibledata			    = CASE WHEN mdm_manualchange = 1 THEN column_sensibledata ELSE ${if (p_Column_SensibleData) "1" else "0"}	END		
        		 ,column_enabledtlog			    = ${if (p_Column_EnableDTLog) "1" else "0"}
        		 ,column_enableoldvalue			  = ${if (p_Column_EnableOldValue) "1" else "0"}
        		 ,column_enableprocesslog		  = ${if (p_Column_EnableProcessLog) "1" else "0"}
        		 ,column_defaultvalue			    = ${ReplaceSQLStringNulls(p_Column_DefaultValue)}			
        		 ,column_securitylevel			  = CASE WHEN mdm_manualchange = 1 THEN column_securitylevel ELSE ${ReplaceSQLStringNulls(p_Column_SecurityLevel)}	END		
        		 ,column_encrypted				    = ${ReplaceSQLStringNulls(p_Column_Encrypted)}				
        		 ,column_arco					        = CASE WHEN mdm_manualchange = 1 THEN column_arco ELSE ${if (p_Column_ARCO) "'1'" else "'0'"}	END				
        		 ,column_nullable				      = ${if (p_Column_Nullable) "1" else "0"}				
        		 ,column_ispk					        = ${if (p_Column_IsPK) "1" else "0"}
        		 ,column_isunique				      = ${if (p_Column_IsUnique) "1" else "0"}
        		 ,column_dq_minlen				    = ${p_Column_DQ_MinLen}				
        		 ,column_dq_maxlen				    = ${p_Column_DQ_MaxLen}				
        		 ,column_dq_mindecimalvalue		= ${p_Column_DQ_MinDecimalValue}		
        		 ,column_dq_maxdecimalvalue		= ${p_Column_DQ_MaxDecimalValue}		
        		 ,column_dq_mindatetimevalue	= ${ReplaceSQLStringNulls(p_Column_DQ_MinDateTimeValue)}	
        		 ,column_dq_maxdatetimevalue	= ${ReplaceSQLStringNulls(p_Column_DQ_MaxDateTimeValue)}	
        		 ,Column_dq_regexp            = ${ReplaceSQLStringNulls(p_Column_DQ_RegExp)}
        		 ,mdm_active					        = 1
          WHERE column_id = ${ReplaceSQLStringNulls(Localcolumn_id)}
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
                      								 ,column_sensibledata
                      								 ,column_enabledtlog
                      								 ,column_enableoldvalue
                      								 ,column_enableprocesslog
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
                      								 ,mdm_fhcreate
                      								 ,mdm_processname
                                       ,mdm_manualchange
                                       ,mdm_active) 	
        	VALUES(    ${ReplaceSQLStringNulls(p_Column_id)}
            			 ,${ReplaceSQLStringNulls(p_Table_id)}
            			 ,${p_Column_Position}
            			 ,${ReplaceSQLStringNulls(p_Column_Name)}
            			 ,${ReplaceSQLStringNulls(p_Column_Description)}
            			 ,${ReplaceSQLStringNulls(p_Column_Formula)}
            			 ,${ReplaceSQLStringNulls(p_Column_DataType)}
            			 ,${if (p_Column_SensibleData) "1" else "0"}
            			 ,${if (p_Column_EnableDTLog) "1" else "0"}
            			 ,${if (p_Column_EnableOldValue) "1" else "0"}
            			 ,${if (p_Column_EnableProcessLog) "1" else "0"}
            			 ,${ReplaceSQLStringNulls(p_Column_DefaultValue)}
            			 ,${ReplaceSQLStringNulls(p_Column_SecurityLevel)}
            			 ,${ReplaceSQLStringNulls(p_Column_Encrypted)}
            			 ,${if (p_Column_ARCO) "'1'" else "'0'"}
            			 ,${if (p_Column_Nullable) "1" else "0"}
            			 ,${if (p_Column_IsPK) "1" else "0"}
            			 ,${if (p_Column_IsUnique) "1" else "0"}
            			 ,${p_Column_DQ_MinLen}
            			 ,${p_Column_DQ_MaxLen}
            			 ,${p_Column_DQ_MinDecimalValue}
            			 ,${p_Column_DQ_MaxDecimalValue}
            			 ,${ReplaceSQLStringNulls(p_Column_DQ_MinDateTimeValue)}
            			 ,${ReplaceSQLStringNulls(p_Column_DQ_MaxDateTimeValue)}
            			 ,${ReplaceSQLStringNulls(p_Column_DQ_RegExp)}
            			 ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
            			 ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
            			 ,0
                   ,1
      )
          """)
          
      ExecResult.OpenVar = p_Column_id
    }
    
    
    return ExecResult             
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
  			  WHERE table_name = ${ReplaceSQLStringNulls(p_Table_NamePK)}
  			  AND   table_bbddname = ${ReplaceSQLStringNulls(p_Table_BBDDPK)}
      """)
    
    var ExecResult: huemul_JDBCResult = null
    var PK_Id: String = null
    if (!ExecResult_PK_Id.IsError && ExecResult_PK_Id.ResultSet.length == 1){
      PK_Id = ExecResult_PK_Id.ResultSet(0).getAs[String]("table_id".toLowerCase())
      
      val ExecResult_Rel_Id = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_WithResult(s"""
          SELECT tablerel_id 
					 FROM control_tablesrel 
					 WHERE table_idpk = ${ReplaceSQLStringNulls(PK_Id)}
					 AND   table_idfk = ${ReplaceSQLStringNulls(p_FK_ID)}
					 AND   tablefk_namerelationship = ${ReplaceSQLStringNulls(p_TableFK_NameRelationship)}
      """)
      
      if (ExecResult_Rel_Id.IsError)
        RaiseError(ExecResult_Rel_Id.ErrorDescription)
        
      var TableRel_id: String = null
      if (ExecResult_Rel_Id.ResultSet.length > 0){
        TableRel_id = ExecResult_Rel_Id.ResultSet(0).getAs[String]("tablerel_id".toLowerCase())
      
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          DELETE FROM control_tablesrelcol WHERE tablerel_id = ${ReplaceSQLStringNulls(TableRel_id)}
          """)
          
        ExecResult = huemulBigDataGov.CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
          UPDATE control_tablesrel 
          SET  tablerel_validNull       = ${if (p_TableRel_ValidNull) "1" else "0"}
          WHERE tablerel_id = ${ReplaceSQLStringNulls(TableRel_id)}
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
      		VALUES( ${ReplaceSQLStringNulls(p_TableRel_id)}
      			   , ${ReplaceSQLStringNulls(PK_Id)}
      			   , ${ReplaceSQLStringNulls(p_FK_ID)}
      			   , ${ReplaceSQLStringNulls(p_TableFK_NameRelationship)}
      			   , ${if (p_TableRel_ValidNull) "1" else "0"}
      			   , 0
      			   , ${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
      			   , ${ReplaceSQLStringNulls(p_mdm_processname)}
          )      			   
          """)
        ExecResult.OpenVar = p_TableRel_id
        ExecResult.OpenVar2 = PK_Id
      }
    }
    
    
    
    return ExecResult             
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
				  WHERE table_id    = ${ReplaceSQLStringNulls(p_PK_ID)}
				  AND   column_name = ${ReplaceSQLStringNulls(p_ColumnName_PK)}
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
				  WHERE table_id    = ${ReplaceSQLStringNulls(p_FK_ID)}
				  AND   column_name = ${ReplaceSQLStringNulls(p_ColumnName_FK)}
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
        	VALUES( ${ReplaceSQLStringNulls(p_TableRel_id)}
          		  ,${ReplaceSQLStringNulls(ColumnFK)}
          		  ,${ReplaceSQLStringNulls(ColumnPK)}
          		  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
          		  ,${ReplaceSQLStringNulls(p_mdm_processname)}
          )          		  
      """)
      
    
    
    
    return ExecResult             
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
      
    
    return ExecResult             
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
        	VALUES( ${ReplaceSQLStringNulls(p_Error_Id)}
          		  ,${ReplaceSQLStringNulls(p_Error_Message)}
                ,${p_Error_Code}
          		  ,${ReplaceSQLStringNulls(p_Error_Trace)}
          		  ,${ReplaceSQLStringNulls(p_Error_ClassName)}
          		  ,${ReplaceSQLStringNulls(p_Error_FileName)}
          		  ,${ReplaceSQLStringNulls(p_Error_LIneNumber)}
          		  ,${ReplaceSQLStringNulls(p_Error_MethodName)}
          		  ,${ReplaceSQLStringNulls(p_Error_Detail)}
          		  ,${ReplaceSQLStringNulls(huemulBigDataGov.getCurrentDateTime())}
          		  ,${ReplaceSQLStringNulls(p_MDM_ProcessName)}
          )          		  
      """)
       
    
    return ExecResult             
  }
}